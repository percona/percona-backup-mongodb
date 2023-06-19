package restore

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/sel"
	"github.com/percona/percona-backup-mongodb/pbm/snapshot"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/version"
)

type Restore struct {
	name     string
	cn       *pbm.PBM
	node     *pbm.Node
	stopHB   chan struct{}
	nodeInfo *pbm.NodeInfo
	stg      storage.Storage
	// Shards to participate in restore. Num of shards in bcp could
	// be less than in the cluster and this is ok. Only these shards
	// would be expected to run restore (distributed transactions sync,
	//  status checks etc.)
	//
	// Only the restore leader would have this info.
	shards []pbm.Shard
	// rsMap is mapping between old and new replset names. used for data restore.
	// empty if all replset names are the same
	rsMap map[string]string
	// sMap is mapping between old and new shard names. used for router config update.
	// empty if all shard names are the same
	sMap map[string]string

	oplog *oplog.OplogRestore
	log   *log.Event
	opid  string
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
	}
}

// Close releases object resources.
// Should be run to avoid leaks.
func (r *Restore) Close() {
	if r.stopHB != nil {
		close(r.stopHB)
	}
}

func (r *Restore) exit(err error, l *log.Event) {
	if err != nil && !errors.Is(err, ErrNoDataForShard) {
		ferr := r.MarkFailed(err)
		if ferr != nil {
			l.Error("mark restore as failed `%v`: %v", err, ferr)
		}
	}

	r.Close()
}

// Snapshot do the snapshot's (mongo dump) restore
func (r *Restore) Snapshot(cmd *pbm.RestoreCmd, opid pbm.OPID, l *log.Event) (err error) {
	defer func() { r.exit(err, l) }() // !!! has to be in a closure

	bcp, err := SnapshotMeta(r.cn, cmd.BackupName, r.stg)
	if err != nil {
		return err
	}

	err = r.init(cmd.Name, opid, l)
	if err != nil {
		return err
	}

	nss := cmd.Namespaces
	if !sel.IsSelective(nss) {
		nss = bcp.Namespaces
	}

	err = r.cn.SetRestoreBackup(r.name, cmd.BackupName, nss)
	if err != nil {
		return errors.Wrap(err, "set backup name")
	}

	err = r.checkSnapshot(bcp)
	if err != nil {
		return err
	}

	err = r.setShards(bcp)
	if err != nil {
		return err
	}

	if r.nodeInfo.IsConfigSrv() {
		r.sMap = r.getShardMapping(bcp)
	}

	dump, oplog, err := r.snapshotObjects(bcp)
	if err != nil {
		return err
	}

	err = r.toState(pbm.StatusRunning, &pbm.WaitActionStart)
	if err != nil {
		return err
	}

	err = r.RunSnapshot(dump, bcp, nss)
	if err != nil {
		return err
	}

	err = r.toState(pbm.StatusDumpDone, nil)
	if err != nil {
		return err
	}

	oplogOption := &applyOplogOption{nss: nss}
	if r.nodeInfo.IsConfigSrv() && sel.IsSelective(nss) {
		oplogOption.nss = []string{"config.databases"}
		oplogOption.filter = newConfigsvrOpFilter(nss)
	}

	err = r.applyOplog([]pbm.OplogChunk{{
		RS:          r.nodeInfo.SetName,
		FName:       oplog,
		Compression: bcp.Compression,
		StartTS:     bcp.FirstWriteTS,
		EndTS:       bcp.LastWriteTS,
	}}, oplogOption)
	if err != nil {
		return err
	}

	if err = r.updateRouterConfig(r.cn.Context()); err != nil {
		return errors.WithMessage(err, "update router config")
	}

	return r.Done()
}

// newConfigsvrOpFilter filters out not needed ops during selective backup on configsvr
func newConfigsvrOpFilter(nss []string) oplog.OpFilter {
	selected := sel.MakeSelectedPred(nss)

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
func (r *Restore) PITR(cmd *pbm.RestoreCmd, opid pbm.OPID, l *log.Event) (err error) {
	defer func() { r.exit(err, l) }() // !!! has to be in a closure

	err = r.init(cmd.Name, opid, l)
	if err != nil {
		return err
	}

	// tsTo := primitive.Timestamp{T: uint32(cmd.TS), I: uint32(cmd.I)}
	bcp, err := GetBaseBackup(r.cn, cmd.BackupName, cmd.OplogTS, r.stg)
	if err != nil {
		return errors.Wrap(err, "get base backup")
	}

	nss := cmd.Namespaces
	if len(nss) == 0 {
		nss = bcp.Namespaces
	}

	if r.nodeInfo.IsLeader() {
		err = r.cn.SetOplogTimestamps(r.name, 0, int64(cmd.OplogTS.T))
		if err != nil {
			return errors.Wrap(err, "set PITR timestamp")
		}
	}

	err = r.cn.SetRestoreBackup(r.name, bcp.Name, nss)
	if err != nil {
		return errors.Wrap(err, "set backup name")
	}

	err = r.checkSnapshot(bcp)
	if err != nil {
		return err
	}

	err = r.setShards(bcp)
	if err != nil {
		return err
	}

	bcpShards := make([]string, len(bcp.Replsets))
	for i := range bcp.Replsets {
		bcpShards[i] = bcp.Replsets[i].Name
	}

	if !Contains(bcpShards, pbm.MakeReverseRSMapFunc(r.rsMap)(r.nodeInfo.SetName)) {
		return r.Done() // skip. no backup for current rs
	}

	if r.nodeInfo.IsConfigSrv() {
		r.sMap = r.getShardMapping(bcp)
	}

	chunks, err := r.chunks(bcp.LastWriteTS, cmd.OplogTS)
	if err != nil {
		return err
	}

	dump, oplog, err := r.snapshotObjects(bcp)
	if err != nil {
		return err
	}

	err = r.toState(pbm.StatusRunning, &pbm.WaitActionStart)
	if err != nil {
		return err
	}

	err = r.RunSnapshot(dump, bcp, nss)
	if err != nil {
		return err
	}

	err = r.toState(pbm.StatusDumpDone, nil)
	if err != nil {
		return err
	}

	snapshotChunk := pbm.OplogChunk{
		RS:          r.nodeInfo.SetName,
		FName:       oplog,
		Compression: bcp.Compression,
		StartTS:     bcp.FirstWriteTS,
		EndTS:       bcp.LastWriteTS,
	}

	oplogOption := applyOplogOption{end: &cmd.OplogTS, nss: nss}
	if r.nodeInfo.IsConfigSrv() && sel.IsSelective(nss) {
		oplogOption.nss = []string{"config.databases"}
		oplogOption.filter = newConfigsvrOpFilter(nss)
	}

	err = r.applyOplog(append([]pbm.OplogChunk{snapshotChunk}, chunks...), &oplogOption)
	if err != nil {
		return err
	}

	if err = r.updateRouterConfig(r.cn.Context()); err != nil {
		return errors.WithMessage(err, "update router config")
	}

	return r.Done()
}

func (r *Restore) ReplayOplog(cmd *pbm.ReplayCmd, opid pbm.OPID, l *log.Event) (err error) {
	defer func() { r.exit(err, l) }() // !!! has to be in a closure

	if err = r.init(cmd.Name, opid, l); err != nil {
		return errors.Wrap(err, "init")
	}

	if !r.nodeInfo.IsPrimary {
		return errors.Errorf("%q is not primary", r.nodeInfo.SetName)
	}

	r.shards, err = r.cn.ClusterMembers()
	if err != nil {
		return errors.Wrap(err, "get cluster members")
	}

	if r.nodeInfo.IsLeader() {
		err := r.cn.SetOplogTimestamps(r.name, int64(cmd.Start.T), int64(cmd.End.T))
		if err != nil {
			return errors.Wrap(err, "set oplog timestamps")
		}
	}

	oplogShards, err := r.cn.AllOplogRSNames(r.cn.Context(), cmd.Start, cmd.End)
	if err != nil {
		return err
	}

	err = r.checkTopologyForOplog(r.cn.Context(), r.shards, oplogShards)
	if err != nil {
		return errors.WithMessage(err, "topology")
	}

	if !Contains(oplogShards, pbm.MakeReverseRSMapFunc(r.rsMap)(r.nodeInfo.SetName)) {
		return r.Done() // skip. no oplog for current rs
	}

	opChunks, err := r.chunks(cmd.Start, cmd.End)
	if err != nil {
		return err
	}

	err = r.toState(pbm.StatusRunning, &pbm.WaitActionStart)
	if err != nil {
		return err
	}

	oplogOption := applyOplogOption{
		start:  &cmd.Start,
		end:    &cmd.End,
		unsafe: true,
	}
	if err = r.applyOplog(opChunks, &oplogOption); err != nil {
		return err
	}

	return r.Done()
}

func (r *Restore) init(name string, opid pbm.OPID, l *log.Event) (err error) {
	r.log = l

	r.nodeInfo, err = r.node.GetInfo()
	if err != nil {
		return errors.Wrap(err, "get node data")
	}
	if r.nodeInfo.SetName == "" {
		return errors.Wrap(err, "unable to define replica set")
	}

	r.name = name
	r.opid = opid.String()
	if r.nodeInfo.IsLeader() {
		ts, err := r.cn.ClusterTime()
		if err != nil {
			return errors.Wrap(err, "init restore meta, read cluster time")
		}

		meta := &pbm.RestoreMeta{
			Type:     pbm.LogicalBackup,
			OPID:     r.opid,
			Name:     r.name,
			StartTS:  time.Now().Unix(),
			Status:   pbm.StatusStarting,
			Replsets: []pbm.RestoreReplset{},
			Hb:       ts,
		}
		err = r.cn.SetRestoreMeta(meta)
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
					err := r.cn.RestoreHB(r.name)
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
	err = r.waitForStatus(pbm.StatusStarting)
	if err != nil {
		return errors.Wrap(err, "waiting for start")
	}

	rsMeta := pbm.RestoreReplset{
		Name:       r.nodeInfo.SetName,
		StartTS:    time.Now().UTC().Unix(),
		Status:     pbm.StatusStarting,
		Conditions: pbm.Conditions{},
	}

	err = r.cn.AddRestoreRSMeta(r.name, rsMeta)
	if err != nil {
		return errors.Wrap(err, "add shard's metadata")
	}

	r.stg, err = r.cn.GetStorage(r.log)
	if err != nil {
		return errors.Wrap(err, "get backup storage")
	}

	return nil
}

func (r *Restore) checkTopologyForOplog(ctx context.Context, currShards []pbm.Shard, oplogShards []string) error {
	mapRS, mapRevRS := pbm.MakeRSMapFunc(r.rsMap), pbm.MakeReverseRSMapFunc(r.rsMap)

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
func (r *Restore) chunks(from, to primitive.Timestamp) ([]pbm.OplogChunk, error) {
	return chunks(r.cn, r.stg, from, to, r.nodeInfo.SetName, r.rsMap)
}

func SnapshotMeta(cn *pbm.PBM, backupName string, stg storage.Storage) (bcp *pbm.BackupMeta, err error) {
	bcp, err = cn.GetBackupMeta(backupName)
	if errors.Is(err, pbm.ErrNotFound) {
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
func (r *Restore) setShards(bcp *pbm.BackupMeta) error {
	s, err := r.cn.ClusterMembers()
	if err != nil {
		return errors.Wrap(err, "get cluster members")
	}

	fl := make(map[string]pbm.Shard, len(s))
	for _, rs := range s {
		fl[rs.RS] = rs
	}

	mapRS, mapRevRS := pbm.MakeRSMapFunc(r.rsMap), pbm.MakeReverseRSMapFunc(r.rsMap)

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

func (r *Restore) snapshotObjects(bcp *pbm.BackupMeta) (dump, oplog string, err error) {
	mapRS := pbm.MakeRSMapFunc(r.rsMap)

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

func (r *Restore) checkSnapshot(bcp *pbm.BackupMeta) error {
	if bcp.Status != pbm.StatusDone {
		return errors.Errorf("backup wasn't successful: status: %s, error: %s", bcp.Status, bcp.Error())
	}

	if !version.CompatibleWith(version.DefaultInfo.Version, pbm.BreakingChangesMap[bcp.Type]) {
		return errors.Errorf("backup PBM v%s is incompatible with the running PBM v%s", bcp.PBMVersion, version.DefaultInfo.Version)
	}

	if bcp.FCV != "" {
		fcv, err := r.node.GetFeatureCompatibilityVersion()
		if err != nil {
			return errors.WithMessage(err, "get featureCompatibilityVersion")
		}

		if bcp.FCV != fcv {
			r.log.Warning("backup FCV %q is incompatible with the running mongo FCV %q",
				bcp.FCV, fcv)
			return nil
		}
	} else {
		ver, err := r.node.GetMongoVersion()
		if err != nil {
			return errors.WithMessage(err, "get mongo version")
		}

		if majmin(bcp.MongoVersion) != majmin(ver.VersionString) {
			r.log.Warning("backup mongo version %q is incompatible with the running mongo version %q",
				bcp.MongoVersion, ver.VersionString)
			return nil
		}
	}

	return nil
}

func (r *Restore) toState(status pbm.Status, wait *time.Duration) error {
	r.log.Info("moving to state %s", status)
	_, err := toState(r.cn, status, r.name, r.nodeInfo, r.reconcileStatus, wait)
	return err
}

func (r *Restore) RunSnapshot(dump string, bcp *pbm.BackupMeta, nss []string) (err error) {
	var rdr io.ReadCloser

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
		if !sel.IsSelective(nss) {
			nss = bcp.Namespaces
		}
		if !sel.IsSelective(nss) {
			nss = []string{"*.*"}
		}

		mapRS := pbm.MakeReverseRSMapFunc(r.rsMap)
		if r.nodeInfo.IsConfigSrv() && sel.IsSelective(nss) {
			// restore cluster specific configs only
			return r.configsvrRestore(bcp, nss, mapRS)
		}

		var cfg pbm.Config
		// get pbm.Config for creating a storage.Storage later.
		// while r.stg is already created storage for the restore,
		// it triggers data race warnings during concurrent file downloading/reading.
		// for that, it's better to create a new storage for each file
		cfg, err = r.cn.GetConfig()
		if err != nil {
			return errors.WithMessage(err, "get config")
		}

		rdr, err = snapshot.DownloadDump(
			func(ns string) (io.ReadCloser, error) {
				stg, err := pbm.Storage(cfg, r.log)
				if err != nil {
					return nil, errors.WithMessage(err, "get storage")
				}
				// while importing backup made by RS with another name
				// that current RS we can't use our r.node.RS() to point files
				// we have to use mapping passed by --replset-mapping option
				return stg.SourceReader(path.Join(bcp.Name, mapRS(r.node.RS()), ns))
			},
			bcp.Compression,
			sel.MakeSelectedPred(nss))
	}
	if err != nil {
		return err
	}
	defer rdr.Close()

	// Restore snapshot (mongorestore)
	err = r.snapshot(rdr)
	if err != nil {
		return errors.Wrap(err, "mongorestore")
	}

	if sel.IsSelective(nss) {
		return nil
	}

	r.log.Info("restoring users and roles")
	cusr, err := r.node.CurrentUser()
	if err != nil {
		return errors.Wrap(err, "get current user")
	}

	err = r.swapUsers(r.cn.Context(), cusr)
	if err != nil {
		return errors.Wrap(err, "swap users 'n' roles")
	}

	err = pbm.DropTMPcoll(r.cn.Context(), r.node.Session())
	if err != nil {
		r.log.Warning("drop tmp collections: %v", err)
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

	res := r.cn.Conn.Database(pbm.DB).RunCommand(ctx, primitive.M{"flushRouterConfig": 1})
	return errors.WithMessage(res.Err(), "flushRouterConfig")
}

func updateRouterTables(ctx context.Context, m *mongo.Client, sMap map[string]string) error {
	if err := updateDatabasesRouterTable(ctx, m, sMap); err != nil {
		return errors.WithMessage(err, "databases")
	}

	if err := updateChunksRouterTable(ctx, m, sMap); err != nil {
		return errors.WithMessage(err, "chunks")
	}

	return nil
}

func updateDatabasesRouterTable(ctx context.Context, m *mongo.Client, sMap map[string]string) error {
	coll := m.Database("config").Collection("databases")

	oldNames := make(primitive.A, 0, len(sMap))
	for k := range sMap {
		oldNames = append(oldNames, k)
	}

	q := primitive.M{"primary": primitive.M{"$in": oldNames}}
	cur, err := coll.Find(ctx, q)
	if err != nil {
		return errors.WithMessage(err, "query")
	}

	models := make([]mongo.WriteModel, 0)
	for cur.Next(ctx) {
		var doc struct {
			ID      string `bson:"_id"`
			Primary string `bson:"primary"`
		}
		if err := cur.Decode(&doc); err != nil {
			return errors.WithMessage(err, "decode")
		}

		m := mongo.NewUpdateOneModel()
		m.SetFilter(primitive.M{"_id": doc.ID})
		m.SetUpdate(primitive.M{"$set": primitive.M{"primary": sMap[doc.Primary]}})

		models = append(models, m)
	}
	if err := cur.Err(); err != nil {
		return errors.WithMessage(err, "cursor")
	}
	if len(models) == 0 {
		return nil
	}

	_, err = coll.BulkWrite(ctx, models)
	return errors.WithMessage(err, "bulk write")
}

func updateChunksRouterTable(ctx context.Context, m *mongo.Client, sMap map[string]string) error {
	coll := m.Database("config").Collection("chunks")

	oldNames := make(primitive.A, 0, len(sMap))
	for k := range sMap {
		oldNames = append(oldNames, k)
	}

	q := primitive.M{"history.shard": primitive.M{"$in": oldNames}}
	cur, err := coll.Find(ctx, q)
	if err != nil {
		return errors.WithMessage(err, "query")
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
			return errors.WithMessage(err, "decode")
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
		return errors.WithMessage(err, "cursor")
	}
	if len(models) == 0 {
		return nil
	}

	_, err = coll.BulkWrite(ctx, models)
	return errors.WithMessage(err, "bulk write")
}

func (r *Restore) distTxnChecker(done <-chan struct{}, ctxn chan pbm.RestoreTxn, txnSyncErr chan<- error) {
	defer r.log.Debug("exit distTxnChecker")

	for {
		select {
		case txn := <-ctxn:
			r.log.Debug("found dist txn: %s", txn)
			err := r.cn.RestoreSetRSTxn(r.name, r.nodeInfo.SetName, txn)
			if err != nil {
				txnSyncErr <- errors.Wrapf(err, "set transaction %v", txn)
				return
			}

			if txn.State == pbm.TxnCommit {
				txn.State, err = r.txnWaitForShards(txn)
				if err != nil {
					txnSyncErr <- errors.Wrapf(err, "wait transaction %v", txn)
					return
				}
				r.log.Debug("txn converged to [%s] <%s>", txn.State, txn.ID)
				ctxn <- txn
			}
		case <-done:
			return
		}
	}
}

func (r *Restore) waitingTxnChecker(e *error, done <-chan struct{}) {
	defer r.log.Debug("exit waitingTxnChecker")

	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	observedTxn := make(map[string]struct{})
	for {
		select {
		case <-tk.C:
			err := r.checkWaitingTxns(observedTxn)
			if err != nil {
				*e = err
				return
			}
		case <-done:
			return
		}
	}
}

type applyOplogOption struct {
	start  *primitive.Timestamp
	end    *primitive.Timestamp
	nss    []string
	unsafe bool
	filter oplog.OpFilter
}

// In order to sync distributed transactions (commit ontly when all participated shards are committed),
// on all participated in the retore agents:
// distTxnChecker:
// - Receiving distributed transactions from the oplog applier, will add it to the shards restore meta.
// - If txn's state is `commit` it will wait on the rest of the shards for:
//   - either transaction committed on the shard (have `commitTransaction`);
//   - or shard didn't participate in the transaction (more on that below);
//   - or shard participated in the txn (have prepare ops) but no `commitTransaction` by the end of the oplog.
//     If any shard encounters the latter - the transaction is sent back to the applier as aborted.
//     Otherwise - committed.
//
// By looking at just transactions in the oplog we can't tell which shards were participating in it. So given that
// starting `opTime` of the transaction is the same on all shards, we can assume that if some shard(s) oplog applier
// observed greater than the txn's `opTime` and hadn't seen such txn - it wasn't a part of this transaction at all. To
// communicate that, each agent runs `checkWaitingTxns` which in turn periodically checks restore metadata for new
// (unobserved before) waiting transaction events and posts the last observed opTime if there are any. We go with
// `checkWaitingTxns` instead of just updating each observed `opTime`  since the latter would add an extra 1 write
// to the each oplog op on sharded clusters even if there are no dist txns at all.
func (r *Restore) applyOplog(chunks []pbm.OplogChunk, options *applyOplogOption) error {
	r.log.Info("starting oplog replay")
	var err error

	mgoV, err := r.node.GetMongoVersion()
	if err != nil || len(mgoV.Version) < 1 {
		return errors.Wrap(err, "define mongo version")
	}

	var (
		ctxn       chan pbm.RestoreTxn
		txnSyncErr chan error
	)
	if r.nodeInfo.IsSharded() {
		ctxn = make(chan pbm.RestoreTxn)
		txnSyncErr = make(chan error)
	}

	r.oplog, err = oplog.NewOplogRestore(r.node.Session(), mgoV, options.unsafe, true, ctxn, txnSyncErr)
	if err != nil {
		return errors.Wrap(err, "create oplog")
	}

	r.oplog.SetOpFilter(options.filter)

	var startTS, endTS primitive.Timestamp
	if options.start != nil {
		startTS = *options.start
	}
	if options.end != nil {
		endTS = *options.end
	}
	r.oplog.SetTimeframe(startTS, endTS)
	r.oplog.SetIncludeNS(options.nss)

	var waitTxnErr error
	if r.nodeInfo.IsSharded() {
		r.log.Debug("starting sharded txn sync")
		if len(r.shards) == 0 {
			return errors.New("participating shards undefined")
		}
		done := make(chan struct{})
		go r.distTxnChecker(done, ctxn, txnSyncErr)
		go r.waitingTxnChecker(&waitTxnErr, done)
		defer close(done)
	}

	var lts primitive.Timestamp
	for _, chnk := range chunks {
		r.log.Debug("+ applying %v", chnk)

		// If the compression is Snappy and it failed we try S2.
		// Up until v1.7.0 the compression of pitr chunks was always S2.
		// But it was a mess in the code which lead to saving pitr chunk files
		// with the `.snappy`` extension although it was S2 in fact. And during
		// the restore, decompression treated .snappy as S2 ¯\_(ツ)_/¯ It wasn’t
		// an issue since there was no choice. Now, Snappy produces `.snappy` files
		// and S2 - `.s2` which is ok. But this means the old chunks (made by previous
		// PBM versions) won’t be compatible - during the restore, PBM will treat such
		// files as Snappy (judging by its suffix) but in fact, they are s2 files
		// and restore will fail with snappy: corrupt input. So we try S2 in such a case.
		lts, err = r.replayChunk(chnk.FName, chnk.Compression)
		if err != nil && errors.Is(err, snappy.ErrCorrupt) {
			lts, err = r.replayChunk(chnk.FName, compress.CompressionTypeS2)
		}
		if err != nil {
			return errors.Wrapf(err, "replay chunk %v.%v", chnk.StartTS.T, chnk.EndTS.T)
		}

		if waitTxnErr != nil {
			return errors.Wrap(err, "check waiting transactions")
		}
	}

	r.log.Info("oplog replay finished on %v", lts)

	return nil
}

func (r *Restore) checkWaitingTxns(observedTxn map[string]struct{}) error {
	rmeta, err := r.cn.GetRestoreMeta(r.name)
	if err != nil {
		return errors.Wrap(err, "get restore metadata")
	}

	for _, shard := range rmeta.Replsets {
		if _, ok := observedTxn[shard.Txn.ID]; shard.Txn.State == pbm.TxnCommit && !ok {
			ts := primitive.Timestamp{r.oplog.LastOpTS(), 0}
			if primitive.CompareTimestamp(ts, shard.Txn.Ctime) > 0 {
				err := r.cn.SetCurrentOp(r.name, r.nodeInfo.SetName, ts)
				if err != nil {
					return errors.Wrap(err, "set current op timestamp")
				}
			}

			observedTxn[shard.Txn.ID] = struct{}{}
			return nil
		}
	}

	return nil
}

func (r *Restore) txnWaitForShards(txn pbm.RestoreTxn) (pbm.TxnState, error) {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			s, err := r.checkTxn(txn)
			if err != nil {
				return pbm.TxnUnknown, err
			}
			if s == pbm.TxnCommit || s == pbm.TxnAbort {
				return s, nil
			}
		case <-r.cn.Context().Done():
			return pbm.TxnAbort, nil
		}
	}
}

func (r *Restore) checkTxn(txn pbm.RestoreTxn) (pbm.TxnState, error) {
	bmeta, err := r.cn.GetRestoreMeta(r.name)
	if err != nil {
		return pbm.TxnUnknown, errors.Wrap(err, "get restore metadata")
	}

	clusterTime, err := r.cn.ClusterTime()
	if err != nil {
		return pbm.TxnUnknown, errors.Wrap(err, "read cluster time")
	}

	// not going directly thru bmeta.Replsets to be sure we've heard back
	// from all participated in the restore shards.
	shardsToFinish := len(r.shards)
	for _, sh := range r.shards {
		for _, shard := range bmeta.Replsets {
			if shard.Name == sh.RS {
				// check if node alive
				lock, err := r.cn.GetLockData(&pbm.LockHeader{
					Type:    pbm.CmdRestore,
					OPID:    r.opid,
					Replset: shard.Name,
				})

				// nodes are cleaning its locks moving to the done status
				// so no lock is ok and not need to ckech the heartbeats
				if err != mongo.ErrNoDocuments {
					if err != nil {
						return pbm.TxnUnknown, errors.Wrapf(err, "unable to read lock for shard %s", shard.Name)
					}
					if lock.Heartbeat.T+pbm.StaleFrameSec < clusterTime.T {
						return pbm.TxnUnknown, errors.Errorf("lost shard %s, last beat ts: %d", shard.Name, lock.Heartbeat.T)
					}
				}

				if shard.Status == pbm.StatusError {
					return pbm.TxnUnknown, errors.Errorf("shard %s failed with: %v", shard.Name, shard.Error)
				}

				if primitive.CompareTimestamp(shard.CurrentOp, txn.Ctime) == 1 {
					shardsToFinish--
					continue
				}

				if shard.Txn.ID != txn.ID {
					if shard.Status == pbm.StatusDone ||
						primitive.CompareTimestamp(shard.Txn.Ctime, txn.Ctime) == 1 {
						shardsToFinish--
						continue
					}
					return pbm.TxnUnknown, nil
				}

				// check status
				switch shard.Txn.State {
				case pbm.TxnPrepare:
					if shard.Status == pbm.StatusDone {
						return pbm.TxnAbort, nil
					}
					return pbm.TxnUnknown, nil
				case pbm.TxnAbort:
					return pbm.TxnAbort, nil
				case pbm.TxnCommit:
					shardsToFinish--
				}
			}
		}
	}

	if shardsToFinish == 0 {
		return pbm.TxnCommit, nil
	}

	return pbm.TxnUnknown, nil
}

func (r *Restore) snapshot(input io.Reader) (err error) {
	cfg, err := r.cn.GetConfig()
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

func (r *Restore) replayChunk(file string, c compress.CompressionType) (lts primitive.Timestamp, err error) {
	or, err := r.stg.SourceReader(file)
	if err != nil {
		return lts, errors.Wrapf(err, "get object %s form the storage", file)
	}
	defer or.Close()

	oplogReader, err := compress.Decompress(or, c)
	if err != nil {
		return lts, errors.Wrapf(err, "decompress object %s", file)
	}
	defer oplogReader.Close()

	lts, err = r.oplog.Apply(oplogReader)

	return lts, errors.Wrap(err, "apply oplog for chunk")
}

// Done waits for the replicas to finish the job
// and marks restore as done
func (r *Restore) Done() error {
	err := r.cn.ChangeRestoreRSState(r.name, r.nodeInfo.SetName, pbm.StatusDone, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDone")
	}

	if r.nodeInfo.IsLeader() {
		_, err = r.reconcileStatus(pbm.StatusDone, nil)
		if err != nil {
			return errors.Wrap(err, "check cluster for the restore done")
		}
	}

	return nil
}

func (r *Restore) swapUsers(ctx context.Context, exclude *pbm.AuthInfo) error {
	rolesC := r.node.Session().Database("admin").Collection("system.roles")

	eroles := []string{}
	for _, r := range exclude.UserRoles {
		eroles = append(eroles, r.DB+"."+r.Role)
	}

	curr, err := r.node.Session().Database(pbm.DB).Collection(pbm.TmpRolesCollection).Find(ctx, bson.M{"_id": bson.M{"$nin": eroles}})
	if err != nil {
		return errors.Wrap(err, "create cursor for tmpRoles")
	}
	defer curr.Close(ctx)
	_, err = rolesC.DeleteMany(ctx, bson.M{"_id": bson.M{"$nin": eroles}})
	if err != nil {
		return errors.Wrap(err, "delete current roles")
	}

	for curr.Next(ctx) {
		rl := new(interface{})
		err := curr.Decode(rl)
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
	cur, err := r.node.Session().Database(pbm.DB).Collection(pbm.TmpUsersCollection).Find(ctx, bson.M{"_id": bson.M{"$ne": user}})
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
		u := new(interface{})
		err := cur.Decode(u)
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

func (r *Restore) reconcileStatus(status pbm.Status, timeout *time.Duration) (*pbm.RestoreMeta, error) {
	if timeout != nil {
		m, err := convergeClusterWithTimeout(r.cn, r.name, r.opid, r.shards, status, *timeout)
		return m, errors.Wrap(err, "convergeClusterWithTimeout")
	}
	m, err := convergeCluster(r.cn, r.name, r.opid, r.shards, status)
	return m, errors.Wrap(err, "convergeCluster")
}

func (r *Restore) waitForStatus(status pbm.Status) error {
	r.log.Debug("waiting for '%s' status", status)
	return waitForStatus(r.cn, r.name, status)
}

// MarkFailed sets the restore and rs state as failed with the given message
func (r *Restore) MarkFailed(e error) error {
	err := r.cn.ChangeRestoreState(r.name, pbm.StatusError, e.Error())
	if err != nil {
		return errors.Wrap(err, "set restore state")
	}
	err = r.cn.ChangeRestoreRSState(r.name, r.nodeInfo.SetName, pbm.StatusError, e.Error())
	return errors.Wrap(err, "set replset state")
}
