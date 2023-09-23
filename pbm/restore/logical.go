package restore

import (
	"bytes"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/mongodb/mongo-tools/common/bsonutil"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/idx"
	"github.com/mongodb/mongo-tools/mongorestore"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/internal/archive"
	"github.com/percona/percona-backup-mongodb/internal/compress"
	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/lock"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/query"
	"github.com/percona/percona-backup-mongodb/internal/storage"
	"github.com/percona/percona-backup-mongodb/internal/topo"
	"github.com/percona/percona-backup-mongodb/internal/types"
	"github.com/percona/percona-backup-mongodb/internal/util"
	"github.com/percona/percona-backup-mongodb/internal/version"
	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/snapshot"
)

type Restore struct {
	name     string
	cn       *pbm.PBM
	node     *pbm.Node
	stopHB   chan struct{}
	nodeInfo *topo.NodeInfo
	stg      storage.Storage
	// Shards to participate in restore. Num of shards in bcp could
	// be less than in the cluster and this is ok. Only these shards
	// would be expected to run restore (distributed transactions sync,
	//  status checks etc.)
	//
	// Only the restore leader would have this info.
	shards []topo.Shard
	// rsMap is mapping between old and new replset names. used for data restore.
	// empty if all replset names are the same
	rsMap map[string]string
	// sMap is mapping between old and new shard names. used for router config update.
	// empty if all shard names are the same
	sMap map[string]string

	log  *log.Event
	opid string

	indexCatalog *idx.IndexCatalog
}

// New creates a new restore object
func New(cn *pbm.PBM, node *pbm.Node, rsMap map[string]string) *Restore {
	if rsMap == nil {
		rsMap = make(map[string]string)
	}

	return &Restore{
		cn:    cn,
		node:  node,
		rsMap: rsMap,

		indexCatalog: idx.NewIndexCatalog(),
	}
}

// Close releases object resources.
// Should be run to avoid leaks.
func (r *Restore) Close() {
	if r.stopHB != nil {
		close(r.stopHB)
	}
}

func (r *Restore) exit(ctx context.Context, err error, l *log.Event) {
	if err != nil && !errors.Is(err, ErrNoDataForShard) {
		ferr := r.MarkFailed(ctx, err)
		if ferr != nil {
			l.Error("mark restore as failed `%v`: %v", err, ferr)
		}
	}

	r.Close()
}

// Snapshot do the snapshot's (mongo dump) restore
//
//nolint:nonamedreturns
func (r *Restore) Snapshot(ctx context.Context, cmd *types.RestoreCmd, opid types.OPID, l *log.Event) (err error) {
	defer func() { r.exit(ctx, err, l) }()

	bcp, err := SnapshotMeta(ctx, r.cn, cmd.BackupName, r.stg)
	if err != nil {
		return err
	}

	err = r.init(ctx, cmd.Name, opid, l)
	if err != nil {
		return err
	}

	nss := cmd.Namespaces
	if !util.IsSelective(nss) {
		nss = bcp.Namespaces
	}

	err = query.SetRestoreBackup(ctx, r.cn.Conn, r.name, cmd.BackupName, nss)
	if err != nil {
		return errors.Wrap(err, "set backup name")
	}

	err = r.checkSnapshot(ctx, bcp)
	if err != nil {
		return err
	}

	err = r.setShards(ctx, bcp)
	if err != nil {
		return err
	}

	if r.nodeInfo.IsConfigSrv() {
		r.sMap = r.getShardMapping(bcp)
	}

	dump, oplogName, err := r.snapshotObjects(bcp)
	if err != nil {
		return err
	}

	err = r.toState(ctx, defs.StatusRunning, &defs.WaitActionStart)
	if err != nil {
		return err
	}

	err = r.RunSnapshot(ctx, dump, bcp, nss)
	if err != nil {
		return err
	}

	err = r.toState(ctx, defs.StatusDumpDone, nil)
	if err != nil {
		return err
	}

	oplogOption := &applyOplogOption{nss: nss}
	if r.nodeInfo.IsConfigSrv() && util.IsSelective(nss) {
		oplogOption.nss = []string{"config.databases"}
		oplogOption.filter = newConfigsvrOpFilter(nss)
	}

	err = r.applyOplog(ctx, []oplog.OplogChunk{{
		RS:          r.nodeInfo.SetName,
		FName:       oplogName,
		Compression: bcp.Compression,
		StartTS:     bcp.FirstWriteTS,
		EndTS:       bcp.LastWriteTS,
	}}, oplogOption)
	if err != nil {
		return err
	}

	err = r.restoreIndexes(ctx, oplogOption.nss)
	if err != nil {
		return errors.Wrap(err, "restore indexes")
	}

	if err = r.updateRouterConfig(ctx); err != nil {
		return errors.Wrap(err, "update router config")
	}

	return r.Done(ctx)
}

// newConfigsvrOpFilter filters out not needed ops during selective backup on configsvr
func newConfigsvrOpFilter(nss []string) oplog.OpFilter {
	selected := util.MakeSelectedPred(nss)

	return func(r *oplog.Record) bool {
		if r.Namespace != "config.databases" {
			return false
		}

		// create/drop database and movePrimary ops contain o2._id with the database name
		for _, e := range r.Query {
			if e.Key != "_id" {
				continue
			}

			db, _ := e.Value.(string)
			return selected(db)
		}

		return false
	}
}

// PITR do the Point-in-Time Recovery
//
//nolint:nonamedreturns
func (r *Restore) PITR(ctx context.Context, cmd *types.RestoreCmd, opid types.OPID, l *log.Event) (err error) {
	defer func() { r.exit(ctx, err, l) }()

	err = r.init(ctx, cmd.Name, opid, l)
	if err != nil {
		return err
	}

	bcp, err := SnapshotMeta(ctx, r.cn, cmd.BackupName, r.stg)
	if err != nil {
		return errors.Wrap(err, "get base backup")
	}
	if bcp.LastWriteTS.Compare(cmd.OplogTS) >= 0 {
		return errors.New("snapshot's last write is later than the target time. " +
			"Try to set an earlier snapshot. Or leave the snapshot empty so PBM will choose one.")
	}

	nss := cmd.Namespaces
	if len(nss) == 0 {
		nss = bcp.Namespaces
	}

	if r.nodeInfo.IsLeader() {
		err = query.SetOplogTimestamps(ctx, r.cn.Conn, r.name, 0, int64(cmd.OplogTS.T))
		if err != nil {
			return errors.Wrap(err, "set PITR timestamp")
		}
	}

	err = query.SetRestoreBackup(ctx, r.cn.Conn, r.name, bcp.Name, nss)
	if err != nil {
		return errors.Wrap(err, "set backup name")
	}

	err = r.checkSnapshot(ctx, bcp)
	if err != nil {
		return err
	}

	err = r.setShards(ctx, bcp)
	if err != nil {
		return err
	}

	bcpShards := make([]string, len(bcp.Replsets))
	for i := range bcp.Replsets {
		bcpShards[i] = bcp.Replsets[i].Name
	}

	if !Contains(bcpShards, util.MakeReverseRSMapFunc(r.rsMap)(r.nodeInfo.SetName)) {
		return r.Done(ctx) // skip. no backup for current rs
	}

	if r.nodeInfo.IsConfigSrv() {
		r.sMap = r.getShardMapping(bcp)
	}

	chunks, err := r.chunks(ctx, bcp.LastWriteTS, cmd.OplogTS)
	if err != nil {
		return err
	}

	dump, oplogName, err := r.snapshotObjects(bcp)
	if err != nil {
		return err
	}

	err = r.toState(ctx, defs.StatusRunning, &defs.WaitActionStart)
	if err != nil {
		return err
	}

	err = r.RunSnapshot(ctx, dump, bcp, nss)
	if err != nil {
		return err
	}

	err = r.toState(ctx, defs.StatusDumpDone, nil)
	if err != nil {
		return err
	}

	snapshotChunk := oplog.OplogChunk{
		RS:          r.nodeInfo.SetName,
		FName:       oplogName,
		Compression: bcp.Compression,
		StartTS:     bcp.FirstWriteTS,
		EndTS:       bcp.LastWriteTS,
	}

	oplogOption := applyOplogOption{end: &cmd.OplogTS, nss: nss}
	if r.nodeInfo.IsConfigSrv() && util.IsSelective(nss) {
		oplogOption.nss = []string{"config.databases"}
		oplogOption.filter = newConfigsvrOpFilter(nss)
	}

	err = r.applyOplog(ctx, append([]oplog.OplogChunk{snapshotChunk}, chunks...), &oplogOption)
	if err != nil {
		return err
	}

	err = r.restoreIndexes(ctx, oplogOption.nss)
	if err != nil {
		return errors.Wrap(err, "restore indexes")
	}

	if err = r.updateRouterConfig(ctx); err != nil {
		return errors.Wrap(err, "update router config")
	}

	return r.Done(ctx)
}

//nolint:nonamedreturns
func (r *Restore) ReplayOplog(ctx context.Context, cmd *types.ReplayCmd, opid types.OPID, l *log.Event) (err error) {
	defer func() { r.exit(ctx, err, l) }()

	if err = r.init(ctx, cmd.Name, opid, l); err != nil {
		return errors.Wrap(err, "init")
	}

	if !r.nodeInfo.IsPrimary {
		return errors.Errorf("%q is not primary", r.nodeInfo.SetName)
	}

	r.shards, err = topo.ClusterMembers(ctx, r.cn.Conn.UnsafeClient())
	if err != nil {
		return errors.Wrap(err, "get cluster members")
	}

	if r.nodeInfo.IsLeader() {
		err := query.SetOplogTimestamps(ctx, r.cn.Conn, r.name, int64(cmd.Start.T), int64(cmd.End.T))
		if err != nil {
			return errors.Wrap(err, "set oplog timestamps")
		}
	}

	oplogShards, err := oplog.AllOplogRSNames(ctx, r.cn.Conn, cmd.Start, cmd.End)
	if err != nil {
		return err
	}

	err = r.checkTopologyForOplog(r.shards, oplogShards)
	if err != nil {
		return errors.Wrap(err, "topology")
	}

	if !Contains(oplogShards, util.MakeReverseRSMapFunc(r.rsMap)(r.nodeInfo.SetName)) {
		return r.Done(ctx) // skip. no oplog for current rs
	}

	opChunks, err := r.chunks(ctx, cmd.Start, cmd.End)
	if err != nil {
		return err
	}

	err = r.toState(ctx, defs.StatusRunning, &defs.WaitActionStart)
	if err != nil {
		return err
	}

	oplogOption := applyOplogOption{
		start:  &cmd.Start,
		end:    &cmd.End,
		unsafe: true,
	}
	if err = r.applyOplog(ctx, opChunks, &oplogOption); err != nil {
		return err
	}

	return r.Done(ctx)
}

func (r *Restore) init(ctx context.Context, name string, opid types.OPID, l *log.Event) error {
	r.log = l

	var err error
	r.nodeInfo, err = topo.GetNodeInfoExt(ctx, r.node.Session())
	if err != nil {
		return errors.Wrap(err, "get node data")
	}
	if r.nodeInfo.SetName == "" {
		return errors.Wrap(err, "unable to define replica set")
	}

	r.name = name
	r.opid = opid.String()
	if r.nodeInfo.IsLeader() {
		ts, err := topo.GetClusterTime(ctx, r.cn.Conn)
		if err != nil {
			return errors.Wrap(err, "init restore meta, read cluster time")
		}

		meta := &types.RestoreMeta{
			Type:     defs.LogicalBackup,
			OPID:     r.opid,
			Name:     r.name,
			StartTS:  time.Now().Unix(),
			Status:   defs.StatusStarting,
			Replsets: []types.RestoreReplset{},
			Hb:       ts,
		}
		err = query.SetRestoreMeta(ctx, r.cn.Conn, meta)
		if err != nil {
			return errors.Wrap(err, "write backup meta to db")
		}

		r.stopHB = make(chan struct{})
		go func() {
			tk := time.NewTicker(time.Second * 5)
			defer tk.Stop()

			for {
				select {
				case <-tk.C:
					err := query.RestoreHB(ctx, r.cn.Conn, r.name)
					if err != nil {
						l.Error("send heartbeat: %v", err)
					}
				case <-r.stopHB:
					return
				}
			}
		}()
	}

	// Waiting for StatusStarting to move further.
	// In case some preparations has to be done before the restore.
	err = r.waitForStatus(ctx, defs.StatusStarting)
	if err != nil {
		return errors.Wrap(err, "waiting for start")
	}

	rsMeta := types.RestoreReplset{
		Name:       r.nodeInfo.SetName,
		StartTS:    time.Now().UTC().Unix(),
		Status:     defs.StatusStarting,
		Conditions: types.Conditions{},
	}

	err = query.AddRestoreRSMeta(ctx, r.cn.Conn, r.name, rsMeta)
	if err != nil {
		return errors.Wrap(err, "add shard's metadata")
	}

	r.stg, err = util.GetStorage(ctx, r.cn.Conn, r.log)
	if err != nil {
		return errors.Wrap(err, "get backup storage")
	}

	return nil
}

func (r *Restore) checkTopologyForOplog(currShards []topo.Shard, oplogShards []string) error {
	mapRS, mapRevRS := util.MakeRSMapFunc(r.rsMap), util.MakeReverseRSMapFunc(r.rsMap)

	shards := make(map[string]struct{}, len(currShards))
	for i := range r.shards {
		shards[r.shards[i].RS] = struct{}{}
	}

	var missed []string
	for _, rs := range oplogShards {
		name := mapRS(rs)
		if _, ok := shards[name]; !ok {
			missed = append(missed, name)
		} else if mapRevRS(name) != rs {
			missed = append(missed, name)
		}
	}

	if len(missed) != 0 {
		return errors.Errorf("missed replset %s", strings.Join(missed, ", "))
	}

	return nil
}

// chunks defines chunks of oplog slice in given range, ensures its integrity (timeline
// is contiguous - there are no gaps), checks for respective files on storage and returns
// chunks list if all checks passed
func (r *Restore) chunks(ctx context.Context, from, to primitive.Timestamp) ([]oplog.OplogChunk, error) {
	return chunks(ctx, r.cn, r.stg, from, to, r.nodeInfo.SetName, r.rsMap)
}

func SnapshotMeta(ctx context.Context, cn *pbm.PBM, backupName string, stg storage.Storage) (*types.BackupMeta, error) {
	bcp, err := query.GetBackupMeta(ctx, cn.Conn, backupName)
	if errors.Is(err, errors.ErrNotFound) {
		bcp, err = GetMetaFromStore(stg, backupName)
	}
	if err != nil {
		return nil, errors.Wrap(err, "get backup metadata")
	}

	return bcp, nil
}

// setShards defines and set shards participating in the restore
// cluster migth have more shards then the backup and it's ok. But all
// backup's shards must have respective destination on the target cluster.
func (r *Restore) setShards(ctx context.Context, bcp *types.BackupMeta) error {
	s, err := topo.ClusterMembers(ctx, r.cn.Conn.UnsafeClient())
	if err != nil {
		return errors.Wrap(err, "get cluster members")
	}

	fl := make(map[string]topo.Shard, len(s))
	for _, rs := range s {
		fl[rs.RS] = rs
	}

	mapRS, mapRevRS := util.MakeRSMapFunc(r.rsMap), util.MakeReverseRSMapFunc(r.rsMap)

	var nors []string
	for _, sh := range bcp.Replsets {
		name := mapRS(sh.Name)
		rs, ok := fl[name]
		if !ok {
			nors = append(nors, name)
			continue
		} else if mapRevRS(name) != sh.Name {
			nors = append(nors, name)
			continue
		}

		r.shards = append(r.shards, rs)
	}

	if r.nodeInfo.IsLeader() && len(nors) > 0 {
		return errors.Errorf("extra/unknown replica set found in the backup: %s", strings.Join(nors, ", "))
	}

	return nil
}

var ErrNoDataForShard = errors.New("no data for shard")

//nolint:nonamedreturns
func (r *Restore) snapshotObjects(bcp *types.BackupMeta) (dump, oplog string, err error) {
	mapRS := util.MakeRSMapFunc(r.rsMap)

	var ok bool
	for _, v := range bcp.Replsets {
		name := mapRS(v.Name)

		if name == r.nodeInfo.SetName {
			dump = v.DumpName
			oplog = v.OplogName
			ok = true
			break
		}
	}
	if !ok {
		if r.nodeInfo.IsLeader() {
			return "", "", errors.New("no data for the config server or sole rs in backup")
		}
		return "", "", ErrNoDataForShard
	}

	_, err = r.stg.FileStat(dump)
	if err != nil {
		return "", "", errors.Errorf("failed to ensure snapshot file %s: %v", dump, err)
	}

	_, err = r.stg.FileStat(oplog)
	if err != nil {
		return "", "", errors.Errorf("failed to ensure oplog file %s: %v", oplog, err)
	}

	return dump, oplog, nil
}

func (r *Restore) checkSnapshot(ctx context.Context, bcp *types.BackupMeta) error {
	if bcp.Status != defs.StatusDone {
		return errors.Errorf("backup wasn't successful: status: %s, error: %s",
			bcp.Status, bcp.Error())
	}

	if !version.CompatibleWith(version.Current().Version, version.BreakingChangesMap[bcp.Type]) {
		return errors.Errorf("backup PBM v%s is incompatible with the running PBM v%s",
			bcp.PBMVersion, version.Current().Version)
	}

	if bcp.FCV != "" {
		fcv, err := version.GetFCV(ctx, r.node.Session())
		if err != nil {
			return errors.Wrap(err, "get featureCompatibilityVersion")
		}

		if bcp.FCV != fcv {
			r.log.Warning("backup FCV %q is incompatible with the running mongo FCV %q",
				bcp.FCV, fcv)
			return nil
		}
	} else {
		ver, err := version.GetMongoVersion(ctx, r.node.Session())
		if err != nil {
			return errors.Wrap(err, "get mongo version")
		}

		if majmin(bcp.MongoVersion) != majmin(ver.VersionString) {
			r.log.Warning(
				"backup mongo version %q is incompatible with the running mongo version %q",
				bcp.MongoVersion, ver.VersionString)
			return nil
		}
	}

	return nil
}

func (r *Restore) toState(ctx context.Context, status defs.Status, wait *time.Duration) error {
	r.log.Info("moving to state %s", status)
	return toState(ctx, r.cn, status, r.name, r.nodeInfo, r.reconcileStatus, wait)
}

func (r *Restore) RunSnapshot(ctx context.Context, dump string, bcp *types.BackupMeta, nss []string) error {
	var rdr io.ReadCloser

	var err error
	if version.IsLegacyArchive(bcp.PBMVersion) {
		sr, err := r.stg.SourceReader(dump)
		if err != nil {
			return errors.Wrapf(err, "get object %s for the storage", dump)
		}
		defer sr.Close()

		rdr, err = compress.Decompress(sr, bcp.Compression)
		if err != nil {
			return errors.Wrapf(err, "decompress object %s", dump)
		}
	} else {
		if !util.IsSelective(nss) {
			nss = bcp.Namespaces
		}
		if !util.IsSelective(nss) {
			nss = []string{"*.*"}
		}

		mapRS := util.MakeReverseRSMapFunc(r.rsMap)
		if r.nodeInfo.IsConfigSrv() && util.IsSelective(nss) {
			// restore cluster specific configs only
			return r.configsvrRestore(ctx, bcp, nss, mapRS)
		}

		var cfg config.Config
		// get pbm.Config for creating a storage.Storage later.
		// while r.stg is already created storage for the restore,
		// it triggers data race warnings during concurrent file downloading/reading.
		// for that, it's better to create a new storage for each file
		cfg, err = config.GetConfig(ctx, r.cn.Conn)
		if err != nil {
			return errors.Wrap(err, "get config")
		}

		rdr, err = snapshot.DownloadDump(
			func(ns string) (io.ReadCloser, error) {
				stg, err := util.StorageFromConfig(cfg, r.log)
				if err != nil {
					return nil, errors.Wrap(err, "get storage")
				}
				// while importing backup made by RS with another name
				// that current RS we can't use our r.node.RS() to point files
				// we have to use mapping passed by --replset-mapping option
				rdr, err := stg.SourceReader(path.Join(bcp.Name, mapRS(r.node.RS()), ns))
				if err != nil {
					return nil, err
				}

				if ns == archive.MetaFile {
					data, err := io.ReadAll(rdr)
					if err != nil {
						return nil, err
					}

					err = r.loadIndexesFrom(bytes.NewReader(data))
					if err != nil {
						return nil, errors.Wrap(err, "load indexes")
					}

					rdr = io.NopCloser(bytes.NewReader(data))
				}

				return rdr, nil
			},
			bcp.Compression,
			util.MakeSelectedPred(nss))
	}
	if err != nil {
		return err
	}
	defer rdr.Close()

	// Restore snapshot (mongorestore)
	err = r.snapshot(ctx, rdr)
	if err != nil {
		return errors.Wrap(err, "mongorestore")
	}

	if util.IsSelective(nss) {
		return nil
	}

	r.log.Info("restoring users and roles")
	cusr, err := r.node.CurrentUser(ctx)
	if err != nil {
		return errors.Wrap(err, "get current user")
	}

	err = r.swapUsers(ctx, cusr)
	if err != nil {
		return errors.Wrap(err, "swap users 'n' roles")
	}

	err = pbm.DropTMPcoll(ctx, r.node.Session())
	if err != nil {
		r.log.Warning("drop tmp collections: %v", err)
	}

	return nil
}

func (r *Restore) loadIndexesFrom(rdr io.Reader) error {
	meta, err := archive.ReadMetadata(rdr)
	if err != nil {
		return errors.Wrap(err, "read metadata")
	}

	for _, ns := range meta.Namespaces {
		var md mongorestore.Metadata
		err := bson.UnmarshalExtJSON([]byte(ns.Metadata), true, &md)
		if err != nil {
			return errors.Wrapf(err, "unmarshal %s.%s metadata",
				ns.Database, ns.Collection)
		}

		r.indexCatalog.AddIndexes(ns.Database, ns.Collection, md.Indexes)

		simple := true
		if md.Options != nil {
			collation, ok := md.Options.Map()["collation"].(bson.D)
			if ok {
				locale, _ := bsonutil.FindValueByKey("locale", &collation)
				if locale != "" && locale != "simple" {
					simple = false
				}
			}
		}
		if simple {
			r.indexCatalog.SetCollation(ns.Database, ns.Collection, simple)
		}
	}

	return nil
}

func (r *Restore) restoreIndexes(ctx context.Context, nss []string) error {
	r.log.Debug("building indexes up")

	isSelected := util.MakeSelectedPred(nss)
	for _, ns := range r.indexCatalog.Namespaces() {
		if ns := archive.NSify(ns.DB, ns.Collection); !isSelected(ns) {
			r.log.Debug("skip restore indexes for %q", ns)
			continue
		}

		indexes := r.indexCatalog.GetIndexes(ns.DB, ns.Collection)
		for i, index := range indexes {
			if len(index.Key) == 1 && index.Key[0].Key == "_id" {
				// The _id index is already created with the collection
				indexes = append(indexes[:i], indexes[i+1:]...)
				break
			}
		}

		if len(indexes) == 0 {
			r.log.Debug("no indexes for %s.%s", ns.DB, ns.Collection)
			continue
		}

		var indexNames []string
		for _, index := range indexes {
			index.Options["ns"] = ns.DB + "." + ns.Collection
			indexNames = append(indexNames, index.Options["name"].(string))
			// remove the index version, forcing an update
			delete(index.Options, "v")
		}

		rawCommand := bson.D{
			{"createIndexes", ns.Collection},
			{"indexes", indexes},
			{"ignoreUnknownIndexOptions", true},
		}

		r.log.Info("restoring indexes for %s.%s: %s",
			ns.DB, ns.Collection, strings.Join(indexNames, ", "))
		err := r.node.Session().Database(ns.DB).RunCommand(ctx, rawCommand).Err()
		if err != nil {
			return errors.Wrapf(err, "createIndexes for %s.%s", ns.DB, ns.Collection)
		}
	}

	return nil
}

func (r *Restore) updateRouterConfig(ctx context.Context) error {
	if len(r.sMap) == 0 || !r.nodeInfo.IsSharded() {
		return nil
	}

	if r.nodeInfo.IsConfigSrv() {
		r.log.Debug("updating router config")
		if err := updateRouterTables(ctx, r.cn.Conn, r.sMap); err != nil {
			return err
		}
	}

	res := r.cn.Conn.AdminCommand(ctx, primitive.M{"flushRouterConfig": 1})
	return errors.Wrap(res.Err(), "flushRouterConfig")
}

func updateRouterTables(ctx context.Context, m connect.MetaClient, sMap map[string]string) error {
	if err := updateDatabasesRouterTable(ctx, m, sMap); err != nil {
		return errors.Wrap(err, "databases")
	}

	if err := updateChunksRouterTable(ctx, m, sMap); err != nil {
		return errors.Wrap(err, "chunks")
	}

	return nil
}

func updateDatabasesRouterTable(ctx context.Context, m connect.MetaClient, sMap map[string]string) error {
	coll := m.ConfigDatabase().Collection("databases")

	oldNames := make(primitive.A, 0, len(sMap))
	for k := range sMap {
		oldNames = append(oldNames, k)
	}

	q := primitive.M{"primary": primitive.M{"$in": oldNames}}
	cur, err := coll.Find(ctx, q)
	if err != nil {
		return errors.Wrap(err, "query")
	}

	models := make([]mongo.WriteModel, 0)
	for cur.Next(ctx) {
		var doc struct {
			ID      string `bson:"_id"`
			Primary string `bson:"primary"`
		}
		if err := cur.Decode(&doc); err != nil {
			return errors.Wrap(err, "decode")
		}

		m := mongo.NewUpdateOneModel()
		m.SetFilter(primitive.M{"_id": doc.ID})
		m.SetUpdate(primitive.M{"$set": primitive.M{"primary": sMap[doc.Primary]}})

		models = append(models, m)
	}
	if err := cur.Err(); err != nil {
		return errors.Wrap(err, "cursor")
	}
	if len(models) == 0 {
		return nil
	}

	_, err = coll.BulkWrite(ctx, models)
	return errors.Wrap(err, "bulk write")
}

func updateChunksRouterTable(ctx context.Context, m connect.MetaClient, sMap map[string]string) error {
	coll := m.ConfigDatabase().Collection("chunks")

	oldNames := make(primitive.A, 0, len(sMap))
	for k := range sMap {
		oldNames = append(oldNames, k)
	}

	q := primitive.M{"history.shard": primitive.M{"$in": oldNames}}
	cur, err := coll.Find(ctx, q)
	if err != nil {
		return errors.Wrap(err, "query")
	}

	models := make([]mongo.WriteModel, 0)
	for cur.Next(ctx) {
		var doc struct {
			ID      any    `bson:"_id"`
			Shard   string `bson:"shard"`
			History []struct {
				Shard string `bson:"shard"`
			} `bson:"history"`
		}
		if err := cur.Decode(&doc); err != nil {
			return errors.Wrap(err, "decode")
		}

		updates := primitive.M{}
		if n, ok := sMap[doc.Shard]; ok {
			updates["shard"] = n
		}

		for i, h := range doc.History {
			if n, ok := sMap[h.Shard]; ok {
				updates[fmt.Sprintf("history.%d.shard", i)] = n
			}
		}

		m := mongo.NewUpdateOneModel()
		m.SetFilter(primitive.M{"_id": doc.ID})
		m.SetUpdate(primitive.M{"$set": updates})
		models = append(models, m)
	}
	if err := cur.Err(); err != nil {
		return errors.Wrap(err, "cursor")
	}
	if len(models) == 0 {
		return nil
	}

	_, err = coll.BulkWrite(ctx, models)
	return errors.Wrap(err, "bulk write")
}

func (r *Restore) setcommittedTxn(ctx context.Context, txn []types.RestoreTxn) error {
	return query.RestoreSetRSTxn(ctx, r.cn.Conn, r.name, r.nodeInfo.SetName, txn)
}

func (r *Restore) getcommittedTxn(ctx context.Context) (map[string]primitive.Timestamp, error) {
	txn := make(map[string]primitive.Timestamp)

	shards := make(map[string]struct{})
	for _, s := range r.shards {
		shards[s.RS] = struct{}{}
	}

	for len(shards) > 0 {
		bmeta, err := query.GetRestoreMeta(ctx, r.cn.Conn, r.name)
		if err != nil {
			return nil, errors.Wrap(err, "get restore metadata")
		}

		clusterTime, err := topo.GetClusterTime(ctx, r.cn.Conn)
		if err != nil {
			return nil, errors.Wrap(err, "read cluster time")
		}

		// not going directly thru bmeta.Replsets to be sure we've heard back
		// from all participated in the restore shards.
		for _, shard := range bmeta.Replsets {
			if _, ok := shards[shard.Name]; !ok {
				continue
			}
			// check if node alive
			lck, err := lock.GetLockData(ctx, r.cn.Conn, &lock.LockHeader{
				Type:    defs.CmdRestore,
				OPID:    r.opid,
				Replset: shard.Name,
			})

			// nodes are cleaning its locks moving to the done status
			// so no lock is ok, and no need to check the heartbeats
			if !errors.Is(err, mongo.ErrNoDocuments) {
				if err != nil {
					return nil, errors.Wrapf(err, "unable to read lock for shard %s", shard.Name)
				}
				if lck.Heartbeat.T+defs.StaleFrameSec < clusterTime.T {
					return nil, errors.Errorf("lost shard %s, last beat ts: %d", shard.Name, lck.Heartbeat.T)
				}
			}

			if shard.Status == defs.StatusError {
				return nil, errors.Errorf("shard %s failed with: %v", shard.Name, shard.Error)
			}

			if shard.CommittedTxnSet {
				for _, t := range shard.CommittedTxn {
					if t.State == types.TxnCommit {
						txn[t.ID] = t.Ctime
					}
				}
				delete(shards, shard.Name)
			}
		}
		time.Sleep(time.Second * 1)
	}

	return txn, nil
}

func (r *Restore) applyOplog(ctx context.Context, chunks []oplog.OplogChunk, options *applyOplogOption) error {
	mgoV, err := version.GetMongoVersion(ctx, r.node.Session())
	if err != nil || len(mgoV.Version) < 1 {
		return errors.Wrap(err, "define mongo version")
	}
	stat := types.RestoreShardStat{}
	partial, err := applyOplog(ctx, r.node.Session(), chunks, options, r.nodeInfo.IsSharded(),
		r.indexCatalog, r.setcommittedTxn, r.getcommittedTxn, &stat.Txn,
		&mgoV, r.stg, r.log)
	if err != nil {
		return errors.Wrap(err, "reply oplog")
	}

	if len(partial) > 0 {
		tops := []db.Oplog{}
		for _, t := range partial {
			tops = append(tops, t.Oplog...)
		}

		err = query.RestoreSetRSPartTxn(ctx, r.cn.Conn, r.name, r.nodeInfo.SetName, tops)
		if err != nil {
			return errors.Wrap(err, "set partial transactions")
		}
	}

	err = query.RestoreSetRSStat(ctx, r.cn.Conn, r.name, r.nodeInfo.SetName, stat)
	if err != nil {
		r.log.Warning("applyOplog: failed to set stat: %v", err)
	}

	return nil
}

func (r *Restore) snapshot(ctx context.Context, input io.Reader) error {
	cfg, err := config.GetConfig(ctx, r.cn.Conn)
	if err != nil {
		return errors.Wrap(err, "unable to get PBM config settings")
	}

	rf, err := snapshot.NewRestore(r.node.ConnURI(), &cfg)
	if err != nil {
		return err
	}

	_, err = rf.ReadFrom(input)
	return err
}

// Done waits for the replicas to finish the job
// and marks restore as done
func (r *Restore) Done(ctx context.Context) error {
	err := query.ChangeRestoreRSState(ctx, r.cn.Conn, r.name, r.nodeInfo.SetName, defs.StatusDone, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDone")
	}

	if r.nodeInfo.IsLeader() {
		err = r.reconcileStatus(ctx, defs.StatusDone, nil)
		if err != nil {
			return errors.Wrap(err, "check cluster for the restore done")
		}

		m, err := query.GetRestoreMeta(ctx, r.cn.Conn, r.name)
		if err != nil {
			return errors.Wrap(err, "update stat: get restore meta")
		}
		if m == nil {
			return nil
		}

		stat := make(map[string]map[string]types.RestoreRSMetrics)

		for _, rs := range m.Replsets {
			stat[rs.Name] = map[string]types.RestoreRSMetrics{
				"_primary": {DistTxn: types.DistTxnStat{
					Partial:          rs.Stat.Txn.Partial,
					ShardUncommitted: rs.Stat.Txn.ShardUncommitted,
					LeftUncommitted:  rs.Stat.Txn.LeftUncommitted,
				}},
			}
		}

		err = query.RestoreSetStat(ctx, r.cn.Conn, r.name, types.RestoreStat{RS: stat})
		if err != nil {
			return errors.Wrap(err, "set restore stat")
		}
	}

	return nil
}

func (r *Restore) swapUsers(ctx context.Context, exclude *types.AuthInfo) error {
	rolesC := r.node.Session().Database("admin").Collection("system.roles")

	eroles := []string{}
	for _, r := range exclude.UserRoles {
		eroles = append(eroles, r.DB+"."+r.Role)
	}

	curr, err := r.node.Session().Database(defs.DB).Collection(defs.TmpRolesCollection).
		Find(ctx, bson.M{"_id": bson.M{"$nin": eroles}})
	if err != nil {
		return errors.Wrap(err, "create cursor for tmpRoles")
	}
	defer curr.Close(ctx)

	_, err = rolesC.DeleteMany(ctx, bson.M{"_id": bson.M{"$nin": eroles}})
	if err != nil {
		return errors.Wrap(err, "delete current roles")
	}

	for curr.Next(ctx) {
		var rl any
		err := curr.Decode(&rl)
		if err != nil {
			return errors.Wrap(err, "decode role")
		}
		_, err = rolesC.InsertOne(ctx, rl)
		if err != nil {
			return errors.Wrap(err, "insert role")
		}
	}

	user := ""
	if len(exclude.Users) > 0 {
		user = exclude.Users[0].DB + "." + exclude.Users[0].User
	}
	cur, err := r.node.Session().Database(defs.DB).Collection(defs.TmpUsersCollection).
		Find(ctx, bson.M{"_id": bson.M{"$ne": user}})
	if err != nil {
		return errors.Wrap(err, "create cursor for tmpUsers")
	}
	defer cur.Close(ctx)

	usersC := r.node.Session().Database("admin").Collection("system.users")
	_, err = usersC.DeleteMany(ctx, bson.M{"_id": bson.M{"$ne": user}})
	if err != nil {
		return errors.Wrap(err, "delete current users")
	}

	for cur.Next(ctx) {
		var u any
		err := cur.Decode(&u)
		if err != nil {
			return errors.Wrap(err, "decode user")
		}
		_, err = usersC.InsertOne(ctx, u)
		if err != nil {
			return errors.Wrap(err, "insert user")
		}
	}

	return nil
}

func (r *Restore) reconcileStatus(ctx context.Context, status defs.Status, timeout *time.Duration) error {
	if timeout != nil {
		err := convergeClusterWithTimeout(ctx, r.cn, r.name, r.opid, r.shards, status, *timeout)
		return errors.Wrap(err, "convergeClusterWithTimeout")
	}
	err := convergeCluster(ctx, r.cn, r.name, r.opid, r.shards, status)
	return errors.Wrap(err, "convergeCluster")
}

func (r *Restore) waitForStatus(ctx context.Context, status defs.Status) error {
	r.log.Debug("waiting for '%s' status", status)
	return waitForStatus(ctx, r.cn, r.name, status)
}

// MarkFailed sets the restore and rs state as failed with the given message
func (r *Restore) MarkFailed(ctx context.Context, e error) error {
	err := query.ChangeRestoreState(ctx, r.cn.Conn, r.name, defs.StatusError, e.Error())
	if err != nil {
		return errors.Wrap(err, "set restore state")
	}
	err = query.ChangeRestoreRSState(ctx, r.cn.Conn, r.name, r.nodeInfo.SetName, defs.StatusError, e.Error())
	return errors.Wrap(err, "set replset state")
}
