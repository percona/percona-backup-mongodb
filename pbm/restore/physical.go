package restore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	slog "log"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/mod/semver"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

const (
	defaultRSdbpath   = "/data/db"
	defaultCSRSdbpath = "/data/configdb"

	mongofslock = "mongod.lock"
)

type PhysRestore struct {
	cn     *pbm.PBM
	node   *pbm.Node
	dbpath string
	// an ephemeral port to restart mongod on during the restore
	tmpPort string
	rsConf  *pbm.RSConfig // original replset config
	startTS int64

	name     string
	opid     string
	nodeInfo *pbm.NodeInfo
	stg      storage.Storage
	bcp      *pbm.BackupMeta

	// path to files on a storage the node will sync its
	// state with the resto of the cluster
	syncPathNode    string
	syncPathRS      string
	syncPathCluster string
	syncPathPeers   map[string]struct{}
	// Shards to participate in restore. Num of shards in bcp could
	// be less than in the cluster and this is ok.
	//
	// Only the restore leader would have this info.
	syncPathShards map[string]struct{}

	stopHB chan struct{}

	log *log.Event
}

func NewPhysical(cn *pbm.PBM, node *pbm.Node, inf *pbm.NodeInfo) (*PhysRestore, error) {
	opts, err := node.GetOpts()
	if err != nil {
		return nil, errors.Wrap(err, "get mongo options")
	}
	p := opts.Storage.DBpath
	if p == "" {
		switch inf.ReplsetRole() {
		case pbm.RoleConfigSrv:
			p = defaultCSRSdbpath
		default:
			p = defaultRSdbpath
		}
	}

	rcf, err := node.GetRSconf()
	if err != nil {
		return nil, errors.Wrap(err, "get replset config")
	}

	if inf.SetName == "" {
		return nil, errors.New("undefined replica set")
	}

	tmpPort, err := peekTmpPort()
	if err != nil {
		return nil, errors.Wrap(err, "peek tmp port")
	}

	return &PhysRestore{
		cn:       cn,
		node:     node,
		dbpath:   p,
		rsConf:   rcf,
		nodeInfo: inf,
		tmpPort:  strconv.Itoa(tmpPort),
	}, nil
}

func peekTmpPort() (int, error) {
	const (
		maxPort   = 65535
		startFrom = 28128
	)

	for p := startFrom; p <= maxPort; p++ {
		ln, err := net.Listen("tcp", ":"+strconv.Itoa(p))
		if err == nil {
			ln.Close()
			return p, nil
		}
	}

	return -1, errors.Errorf("can't find unused port in range [%d-%d]", startFrom, maxPort)
}

// Close releases object resources.
// Should be run to avoid leaks.
func (r *PhysRestore) close() {
	if r.stopHB != nil {
		close(r.stopHB)
	}
}

func (r *PhysRestore) flush() error {
	r.log.Debug("shutdown server")
	rsStat, err := r.node.GetReplsetStatus()
	if err != nil {
		return errors.Wrap(err, "get replset status")
	}
	for {
		inf, err := r.node.GetInfo()
		if err != nil {
			return errors.Wrap(err, "get node info")
		}
		// single-node replica set won't stepdown do secondary
		// so we have to shut it down despite of role
		if !inf.IsPrimary || len(rsStat.Members) == 1 {
			err = r.node.Shutdown()
			if err != nil &&
				strings.Contains(err.Error(), // wait a bit and let the node to stepdown
					"(ConflictingOperationInProgress) This node is already in the process of stepping down") {
				return errors.Wrap(err, "shutdown server")
			}
			break
		}
		r.log.Debug("waiting to became secondary")
		time.Sleep(time.Second * 1)
	}

	err = r.waitMgoShutdown()
	if err != nil {
		return errors.Wrap(err, "shutdown")
	}

	r.log.Debug("revome old data")
	err = removeAll(r.dbpath, r.log)
	if err != nil {
		return errors.Wrapf(err, "flush dbpath %s", r.dbpath)
	}

	return nil
}

func (r *PhysRestore) waitMgoShutdown() error {
	r.log.Debug("waiting for the node to shutdown")

	tk := time.NewTicker(time.Second)
	defer tk.Stop()
	for range tk.C {
		f, err := os.Stat(path.Join(r.dbpath, mongofslock))
		if err != nil {
			return errors.Wrapf(err, "check for lock file %s", path.Join(r.dbpath, mongofslock))
		}

		if f.Size() == 0 {
			return nil
		}
	}

	return nil
}

// toState moves cluster to the given restore state.
// All communication happens via files in the restore dir on storage.
// - Each node writes file with the given state (`<restoreDir>/rsID.nodeID.status`).
// - Replset leader (primary node) waits for files from all replicaset' nodes. And
//   writes a status file for the relicaset.
// - Cluster leader (primary node on config server) waits for status files from
//   all replicasets. And sets status file for the cluster.
// - Each node in turn waits for the cluster status file and returns (move further)
//   once it's observed.
func (r *PhysRestore) toState(status pbm.Status) (err error) {
	defer func() {
		if err != nil {
			if r.nodeInfo.IsPrimary {
				serr := r.stg.Save(r.syncPathRS+"."+string(pbm.StatusError),
					errStatus(err), -1)
				if serr != nil {
					r.log.Error("write replset error state `%v`: %v", err, serr)
				}
			}
			if r.nodeInfo.IsClusterLeader() {
				serr := r.stg.Save(r.syncPathCluster+"."+string(pbm.StatusError),
					errStatus(err), -1)
				if serr != nil {
					r.log.Error("write cluster error state `%v`: %v", err, serr)
				}
			}
		}
	}()

	r.log.Info("moving to state %s", status)

	err = r.stg.Save(r.syncPathNode+"."+string(status),
		okStatus(), -1)
	if err != nil {
		return errors.Wrap(err, "write node state")
	}

	if r.nodeInfo.IsPrimary {
		r.log.Info("waiting for `%s` status in rs %v", status, r.syncPathPeers)
		err = r.waitFiles(status, copyMap(r.syncPathPeers))
		if err != nil {
			return errors.Wrap(err, "wait for nodes in rs")
		}

		err = r.stg.Save(r.syncPathRS+"."+string(status),
			okStatus(), -1)
		if err != nil {
			return errors.Wrap(err, "write replset state")
		}
	}

	if r.nodeInfo.IsClusterLeader() {
		r.log.Info("waiting for shards %v", r.syncPathShards)
		err = r.waitFiles(status, copyMap(r.syncPathShards))
		if err != nil {
			return errors.Wrap(err, "wait for shards")
		}

		err = r.stg.Save(r.syncPathCluster+"."+string(status),
			okStatus(), -1)
		if err != nil {
			return errors.Wrap(err, "write replset state")
		}
	}

	r.log.Info("waiting for cluster")
	err = r.waitFiles(status, map[string]struct{}{
		r.syncPathCluster: {},
	})
	if err != nil {
		return errors.Wrap(err, "wait for shards")
	}

	r.log.Debug("converged to state %s", status)

	return nil
}

func errStatus(err error) io.Reader {
	return bytes.NewReader([]byte(
		fmt.Sprintf("%d:%v", time.Now().Unix(), err),
	))
}

func okStatus() io.Reader {
	return bytes.NewReader([]byte(
		fmt.Sprintf("%d", time.Now().Unix()),
	))
}

type nodeErr struct {
	node string
	msg  string
}

func (n nodeErr) Error() string {
	return fmt.Sprintf("%s failed: %s", n.node, n.msg)
}

func copyMap[K comparable, V any](m map[K]V) map[K]V {
	cp := make(map[K]V)
	for k, v := range m {
		cp[k] = v
	}

	return cp
}

func (r *PhysRestore) waitFiles(state pbm.Status, objs map[string]struct{}) (err error) {
	tk := time.NewTicker(time.Second * 5)
	defer tk.Stop()
	for range tk.C {
		for f := range objs {
			errFile := f + "." + string(pbm.StatusError)
			_, err = r.stg.FileStat(errFile)
			if err != nil && err != storage.ErrNotExist {
				return errors.Wrapf(err, "get file %s", errFile)
			}

			if err == nil {
				r, err := r.stg.SourceReader(errFile)
				if err != nil {
					return errors.Wrapf(err, "open error file %s", errFile)
				}

				b, err := io.ReadAll(r)
				r.Close()
				if err != nil {
					return errors.Wrapf(err, "read error file %s", errFile)
				}
				return nodeErr{filepath.Base(f), string(b)}
			}

			err := r.checkHB(f + "." + syncHbSuffix)
			if err != nil {
				return errors.Wrapf(err, "check heartbeat in %s.%s", f, syncHbSuffix)
			}

			okFile := f + "." + string(state)
			_, err = r.stg.FileStat(okFile)
			if err == storage.ErrNotExist {
				continue
			}

			if err != nil && err != storage.ErrEmpty {
				return errors.Wrapf(err, "get file %s", okFile)
			}

			delete(objs, f)
		}

		if len(objs) == 0 {
			return nil
		}
	}

	return storage.ErrNotExist
}

// Snapshot restores data from the physical snapshot.
//
// Initial sync and coordination between nodes happens via `admin.pbmRestore`
// metadata as with logical restores. But later, since mongod being shutdown,
// status sync going via storage (see `PhysRestore.toState`)
//
// Unlike in logical restore, _every_ node of the replicaset is taking part in
// a physical restore. In that way, we avoid logical resync between nodes after
// the restore. Each node in the cluster does:
// - Stores current replicset config and mongod port.
// - Checks backup and stops all routine writes to the db.
// - Stops mongod and wipes out datadir.
// - Copies backup data to the datadir.
// - Starts standalone mongod on ephemeral (tmp) port and reset some internal
//   data also setting one-node replicaset and sets oplogTruncateAfterPoint
//   to the backup's `last write`. `oplogTruncateAfterPoint` set the time up
//   to which journals would be replayed.
// - Starts standalone mongod to recover oplog from journals.
// - Cleans up data and resets replicaset config to the working state.
// - Shuts down mongod and agent (the leader also dumps metadata to the storage).
func (r *PhysRestore) Snapshot(cmd *pbm.RestoreCmd, opid pbm.OPID, l *log.Event) (err error) {
	meta := &pbm.RestoreMeta{
		Type:     pbm.PhysicalBackup,
		OPID:     opid.String(),
		Name:     cmd.Name,
		Backup:   cmd.BackupName,
		StartTS:  time.Now().Unix(),
		Status:   pbm.StatusInit,
		Replsets: []pbm.RestoreReplset{{Name: r.nodeInfo.Me}},
	}
	if r.nodeInfo.IsClusterLeader() {
		meta.Leader = r.nodeInfo.Me + "/" + r.rsConf.ID
	}

	defer func() {
		if err != nil {
			if !errors.Is(err, ErrNoDataForShard) {
				r.MarkFailed(meta, err)
			}
		}

		r.close()
	}()

	err = r.init(cmd.Name, opid, l)
	if err != nil {
		return errors.Wrap(err, "init")
	}

	err = r.prepareBackup(cmd.BackupName)
	if err != nil {
		return err
	}

	err = r.toState(pbm.StatusStarting)
	if err != nil {
		return errors.Wrap(err, "move to running state")
	}
	l.Debug("%s", pbm.StatusStarting)

	// don't write logs to the mongo anymore
	r.cn.Logger().PauseMgo()

	err = r.toState(pbm.StatusRunning)
	if err != nil {
		return errors.Wrapf(err, "moving to state %s", pbm.StatusRunning)
	}

	l.Info("stopping mongod and flushing old data")
	err = r.flush()
	if err != nil {
		return err
	}

	l.Info("copying backup data")
	err = r.copyFiles()
	if err != nil {
		return errors.Wrap(err, "copy files")
	}

	l.Info("preparing data")
	err = r.prepareData()
	if err != nil {
		return errors.Wrap(err, "prepare data")
	}

	l.Info("recovering oplog as standalone")
	err = r.recoverStandalone()
	if err != nil {
		return errors.Wrap(err, "recover oplog as standalone")
	}

	l.Info("clean-up and reset replicaset config")
	err = r.resetRS()
	if err != nil {
		return errors.Wrap(err, "clean-up, rs_reset")
	}

	err = r.toState(pbm.StatusDone)
	if err != nil {
		return errors.Wrapf(err, "moving to state %s", pbm.StatusDone)
	}

	if r.nodeInfo.IsClusterLeader() {
		r.log.Info("writing restore meta")
		err = r.dumpMeta(meta, pbm.StatusDone, "")
		if err != nil {
			return errors.Wrap(err, "writing restore meta to storage")
		}
	}

	return nil
}

func (r *PhysRestore) dumpMeta(meta *pbm.RestoreMeta, s pbm.Status, msg string) error {
	name := fmt.Sprintf("%s/%s.json", pbm.PhysRestoresDir, meta.Name)
	_, err := r.stg.FileStat(name)
	if err == nil {
		r.log.Warning("meta `%s` already exists, trying write %s status with '%s'", name, s, msg)
		return nil
	}
	if err != nil && err != storage.ErrNotExist {
		return errors.Wrapf(err, "check restore meta `%s`", name)
	}

	// We'll try to build as accurate meta as possible but it won't
	// be 100% accurate as not all agents have reported its final state yet
	// The meta generated here is more for debugging porpuses (just in case).
	// `pbm status` and `resync` will always rebuild it from agents' reports
	// not relying solely on this file.
	condsm, err := pbm.GetPhysRestoreMeta(meta.Name, r.stg)
	if err == nil {
		meta.Replsets = condsm.Replsets
		meta.Status = condsm.Status
		meta.LastTransitionTS = condsm.LastTransitionTS
		meta.Error = condsm.Error
		meta.Hb = condsm.Hb
		meta.Conditions = condsm.Conditions
	}
	if err != nil || s == pbm.StatusError {
		ts := time.Now().Unix()
		meta.Status = s
		meta.Conditions = append(meta.Conditions, &pbm.Condition{Timestamp: ts, Status: s})
		meta.LastTransitionTS = ts
		meta.Error = fmt.Sprintf("%s/%s: %s", r.nodeInfo.SetName, r.nodeInfo.Me, msg)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "\t")
	err = enc.Encode(meta)
	if err != nil {
		return errors.Wrap(err, "encode restore meta")
	}
	err = r.stg.Save(name, &buf, buf.Len())
	if err != nil {
		return errors.Wrap(err, "write restore meta")
	}

	return nil
}

func (r *PhysRestore) copyFiles() error {
	for _, rs := range r.bcp.Replsets {
		if rs.Name == r.nodeInfo.SetName {
			for _, f := range rs.Files {
				src := filepath.Join(r.bcp.Name, r.nodeInfo.SetName, f.Name+r.bcp.Compression.Suffix())
				dst := filepath.Join(r.dbpath, f.Name)

				err := os.MkdirAll(filepath.Dir(dst), os.ModeDir|0o700)
				if err != nil {
					return errors.Wrapf(err, "create path %s", filepath.Dir(dst))
				}

				r.log.Info("copy <%s> to <%s>", src, dst)
				sr, err := r.stg.SourceReader(src)
				if err != nil {
					return errors.Wrapf(err, "create source reader for <%s>", src)
				}
				defer sr.Close()

				data, err := Decompress(sr, r.bcp.Compression)
				if err != nil {
					return errors.Wrapf(err, "decompress object %s", src)
				}
				defer data.Close()

				fw, err := os.Create(dst)
				if err != nil {
					return errors.Wrapf(err, "create destination file <%s>", dst)
				}
				defer fw.Close()
				err = os.Chmod(dst, f.Fmode)
				if err != nil {
					return errors.Wrapf(err, "change permissions for file <%s>", dst)
				}

				_, err = io.Copy(fw, data)
				if err != nil {
					return errors.Wrapf(err, "copy file <%s>", dst)
				}
			}
		}
	}
	return nil
}

func (r *PhysRestore) prepareData() error {
	err := startMongo("--dbpath", r.dbpath, "--port", r.tmpPort, "--setParameter", "disableLogicalSessionCacheRefresh=true")
	if err != nil {
		return errors.Wrap(err, "start mongo")
	}

	c, err := conn(r.tmpPort, "")
	if err != nil {
		return errors.Wrap(err, "connect to mongo")
	}

	ctx := context.Background()

	err = c.Database("local").Collection("replset.minvalid").Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "drop replset.minvalid")
	}
	err = c.Database("local").Collection("replset.oplogTruncateAfterPoint").Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "drop replset.oplogTruncateAfterPoint")
	}
	err = c.Database("local").Collection("replset.election").Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "drop replset.election")
	}
	_, err = c.Database("local").Collection("system.replset").DeleteMany(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "delete from system.replset")
	}

	_, err = c.Database("local").Collection("replset.minvalid").InsertOne(ctx,
		bson.M{"_id": primitive.NewObjectID(), "t": -1, "ts": primitive.Timestamp{0, 1}},
	)
	if err != nil {
		return errors.Wrap(err, "insert to replset.minvalid")
	}

	r.log.Debug("oplogTruncateAfterPoint: %v", r.bcp.LastWriteTS)
	_, err = c.Database("local").Collection("replset.oplogTruncateAfterPoint").InsertOne(ctx,
		bson.M{"_id": "oplogTruncateAfterPoint", "oplogTruncateAfterPoint": r.bcp.LastWriteTS},
	)
	if err != nil {
		return errors.Wrap(err, "set oplogTruncateAfterPoint")
	}

	err = shutdown(c)
	if err != nil {
		return errors.Wrap(err, "shutdown mongo")
	}

	return nil
}

func shutdown(c *mongo.Client) error {
	err := c.Database("admin").RunCommand(context.Background(), bson.D{{"shutdown", 1}}).Err()
	if err != nil && !strings.Contains(err.Error(), "socket was unexpectedly closed") {
		return err
	}

	return nil
}

func (r *PhysRestore) recoverStandalone() error {
	err := startMongo("--dbpath", r.dbpath, "--port", r.tmpPort,
		"--setParameter", "recoverFromOplogAsStandalone=true",
		"--setParameter", "takeUnstableCheckpointOnShutdown=true")
	if err != nil {
		return errors.Wrap(err, "start mongo")
	}

	c, err := conn(r.tmpPort, "")
	if err != nil {
		return errors.Wrap(err, "connect to mongo")
	}

	err = shutdown(c)
	if err != nil {
		return errors.Wrap(err, "shutdown mongo")
	}

	return nil
}

func (r *PhysRestore) resetRS() error {
	err := startMongo("--dbpath", r.dbpath, "--port", r.tmpPort)
	if err != nil {
		return errors.Wrap(err, "start mongo")
	}

	c, err := conn(r.tmpPort, "")
	if err != nil {
		return errors.Wrap(err, "connect to mongo")
	}

	ctx := context.Background()

	if r.nodeInfo.IsConfigSrv() {
		err = c.Database("config").Collection("mongos").Drop(ctx)
		if err != nil {
			return errors.Wrap(err, "drop config.mongos")
		}
		err = c.Database("config").Collection("lockpings").Drop(ctx)
		if err != nil {
			return errors.Wrap(err, "drop config.lockpings")
		}
	}

	err = c.Database("config").Collection("cache.collections").Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "drop config.cache.collections")
	}

	err = c.Database("config").Collection("cache.databases").Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "drop config.cache.databases")
	}

	err = c.Database("config").Collection("cache.chunks.config.system.sessions").Drop(ctx)
	if err != nil {
		return errors.Wrap(err, "drop config.cache.chunks.config.system.sessions")
	}

	const retry = 5
	for i := 0; i < retry; i++ {
		err = c.Database("config").Collection("system.sessions").Drop(ctx)
		if err == nil || !strings.Contains(err.Error(), "(BackgroundOperationInProgressForNamespace)") {
			break
		}
		r.log.Debug("drop config.system.sessions: BackgroundOperationInProgressForNamespace, retrying")
		time.Sleep(time.Second * time.Duration(i+1))
	}
	if err != nil {
		return errors.Wrap(err, "drop config.system.sessions")
	}

	_, err = c.Database("local").Collection("system.replset").DeleteMany(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "delete from system.replset")
	}

	_, err = c.Database("local").Collection("system.replset").InsertOne(ctx,
		pbm.RSConfig{
			ID:       r.rsConf.ID,
			CSRS:     r.nodeInfo.IsConfigSrv(),
			Version:  1,
			Members:  r.rsConf.Members,
			Settings: r.rsConf.Settings,
		},
	)
	if err != nil {
		return errors.Wrapf(err, "upate rs.member host to %s", r.nodeInfo.Me)
	}

	// PITR should be turned off after the physical restore. Otherwise, slicing resumes
	// right after the cluster start while the system in the state of the backup's
	// recovery time. No resync yet. Hence the system knows nothing about the recent
	// restore and chunks made after the backup. So it would successfully start slicing
	// and overwrites chunks after the backup.
	if r.nodeInfo.IsLeader() {
		_, err = c.Database(pbm.DB).Collection(pbm.ConfigCollection).UpdateOne(ctx, bson.D{},
			bson.D{{"$set", bson.M{"pitr.enabled": false}}},
		)
		if err != nil {
			return errors.Wrap(err, "turn off pitr")
		}
	}

	err = shutdown(c)
	if err != nil {
		return errors.Wrap(err, "shutdown mongo")
	}

	return nil
}

func conn(port string, rs string) (*mongo.Client, error) {
	ctx := context.Background()

	opts := options.Client().
		SetHosts([]string{"localhost:" + port}).
		SetAppName("pbm-physical-restore").
		SetDirect(true).
		SetConnectTimeout(time.Second * 60)

	if rs != "" {
		opts = opts.SetReplicaSet(rs)
	}

	conn, err := mongo.NewClient(opts)
	if err != nil {
		return nil, errors.Wrap(err, "create mongo client")
	}

	err = conn.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "connect")
	}

	err = conn.Ping(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "ping")
	}

	return conn, nil
}

func startMongo(opts ...string) error {
	cmd := exec.Command("mongod", opts...)
	err := cmd.Start()
	if err != nil {
		return err
	}

	// release process resources
	go func() {
		err := cmd.Wait()
		if err != nil {
			slog.Println("wait/release mongod process:", err)
		}
	}()
	return nil
}

const hbFrameSec = 60 * 2

func (r *PhysRestore) init(name string, opid pbm.OPID, l *log.Event) (err error) {
	r.stg, err = r.cn.GetStorage(r.log)
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	r.log = l

	r.name = name
	r.opid = opid.String()

	r.startTS = time.Now().Unix()

	r.syncPathNode = fmt.Sprintf("%s/%s/rs.%s/node.%s", pbm.PhysRestoresDir, r.name, r.rsConf.ID, r.nodeInfo.Me)
	r.syncPathRS = fmt.Sprintf("%s/%s/rs.%s/rs", pbm.PhysRestoresDir, r.name, r.rsConf.ID)
	r.syncPathCluster = fmt.Sprintf("%s/%s/cluster", pbm.PhysRestoresDir, r.name)
	r.syncPathPeers = make(map[string]struct{})
	for _, m := range r.rsConf.Members {
		r.syncPathPeers[fmt.Sprintf("%s/%s/rs.%s/node.%s", pbm.PhysRestoresDir, r.name, r.rsConf.ID, m.Host)] = struct{}{}
	}

	err = r.hb()
	if err != nil {
		l.Error("send init heartbeat: %v", err)
	}

	r.stopHB = make(chan struct{})
	go func() {
		tk := time.NewTicker(time.Second * hbFrameSec)
		defer func() {
			tk.Stop()
			l.Debug("hearbeats stopped")
		}()
		for {
			select {
			case <-tk.C:
				err := r.hb()
				if err != nil {
					l.Error("send heartbeat: %v", err)
				}
			case <-r.stopHB:
				return
			}
		}
	}()

	return nil
}

const syncHbSuffix = "hb"

func (r *PhysRestore) hb() error {
	ts := time.Now().Unix()

	err := r.stg.Save(r.syncPathNode+"."+syncHbSuffix,
		bytes.NewReader([]byte(strconv.FormatInt(ts, 10))), -1)
	if err != nil {
		return errors.Wrap(err, "write node hb")
	}

	if r.nodeInfo.IsPrimary {
		err = r.stg.Save(r.syncPathRS+"."+syncHbSuffix,
			bytes.NewReader([]byte(strconv.FormatInt(ts, 10))), -1)
		if err != nil {
			return errors.Wrap(err, "write rs hb")
		}
	}

	if r.nodeInfo.IsClusterLeader() {
		err = r.stg.Save(r.syncPathCluster+"."+syncHbSuffix,
			bytes.NewReader([]byte(strconv.FormatInt(ts, 10))), -1)
		if err != nil {
			return errors.Wrap(err, "write rs hb")
		}
	}

	return nil
}

func (r *PhysRestore) checkHB(file string) error {
	ts := time.Now().Unix()

	_, err := r.stg.FileStat(file)
	// compare with restore start if heartbeat files are yet to be created.
	// basically wait another hbFrameSec*2 sec for heartbeat files.
	if errors.Is(err, storage.ErrNotExist) {
		if r.startTS+hbFrameSec*2 < ts {
			return errors.Errorf("stuck, last beat ts: %d", r.startTS)
		}
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "get file stat")
	}

	f, err := r.stg.SourceReader(file)
	if err != nil {
		return errors.Wrap(err, "get hb file")
	}

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return errors.Wrap(err, "read content")
	}

	t, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 0)
	if err != nil {
		return errors.Wrap(err, "decode")
	}

	if t+hbFrameSec*2 < ts {
		return errors.Errorf("stuck, last beat ts: %d", t)
	}

	return nil
}

func (r *PhysRestore) prepareBackup(backupName string) (err error) {
	r.bcp, err = r.cn.GetBackupMeta(backupName)
	if errors.Is(err, pbm.ErrNotFound) {
		r.bcp, err = GetMetaFromStore(r.stg, backupName)
	}
	if err != nil {
		return errors.Wrap(err, "get backup metadata")
	}

	if r.bcp == nil {
		return errors.New("snapshot name doesn't set")
	}

	err = r.cn.SetRestoreBackup(r.name, r.bcp.Name)
	if err != nil {
		return errors.Wrap(err, "set backup name")
	}

	if r.bcp.Status != pbm.StatusDone {
		return errors.Errorf("backup wasn't successful: status: %s, error: %s", r.bcp.Status, r.bcp.Error())
	}

	mgoV, err := r.node.GetMongoVersion()
	if err != nil || len(mgoV.Version) < 1 {
		return errors.Wrap(err, "define mongo version")
	}

	if semver.Compare(majmin(r.bcp.MongoVersion), majmin(mgoV.VersionString)) != 0 {
		return errors.Errorf("backup's Mongo version (%s) is not compatible with Mongo %s", r.bcp.PBMVersion, mgoV.VersionString)
	}

	if !r.nodeInfo.IsPrimary {
		return nil
	}

	if r.nodeInfo.IsClusterLeader() {
		s, err := r.cn.ClusterMembers()
		if err != nil {
			return errors.Wrap(err, "get cluster members")
		}

		fl := make(map[string]pbm.Shard, len(s))
		for _, rs := range s {
			fl[rs.RS] = rs
		}

		r.syncPathShards = make(map[string]struct{})
		var nors []string
		for _, sh := range r.bcp.Replsets {
			rs, ok := fl[sh.Name]
			if !ok {
				nors = append(nors, sh.Name)
				continue
			}

			r.syncPathShards[fmt.Sprintf("%s/%s/rs.%s/rs", pbm.PhysRestoresDir, r.name, rs.RS)] = struct{}{}
		}

		if len(nors) > 0 {
			return errors.Errorf("extra/unknown replica set found in the backup: %s", strings.Join(nors, ","))
		}
	}

	var ok bool
	for _, v := range r.bcp.Replsets {
		if v.Name == r.nodeInfo.SetName {
			ok = true
			break
		}
	}
	if !ok {
		if r.nodeInfo.IsLeader() {
			return errors.New("no data for the config server or sole rs in backup")
		}
		return ErrNoDataForShard
	}

	return nil
}

// MarkFailed sets the restore and rs state as failed with the given message
func (r *PhysRestore) MarkFailed(meta *pbm.RestoreMeta, e error) {
	var nerr nodeErr
	if errors.As(e, &nerr) {
		e = nerr
		meta.Replsets = []pbm.RestoreReplset{{
			Name:   nerr.node,
			Status: pbm.StatusError,
			Error:  nerr.msg,
		}}
	} else if len(meta.Replsets) > 0 {
		meta.Replsets[0].Status = pbm.StatusError
		meta.Replsets[0].Error = e.Error()
	}

	err := r.stg.Save(r.syncPathNode+"."+string(pbm.StatusError),
		errStatus(e), -1)

	if err != nil {
		r.log.Error("write error state `%v` to storage: %v", e, err)
	}

	err = r.dumpMeta(meta, pbm.StatusError, e.Error())
	if err != nil {
		r.log.Error("write restore meta to storage `%v`: %v", e, err)
	}
}

func removeAll(dir string, l *log.Event) error {
	d, err := os.Open(dir)
	if err != nil {
		return errors.Wrap(err, "open dir")
	}
	defer d.Close()

	names, err := d.Readdirnames(-1)
	if err != nil {
		return errors.Wrap(err, "read file names")
	}
	for _, n := range names {
		err = os.RemoveAll(filepath.Join(dir, n))
		if err != nil {
			return errors.Wrapf(err, "remove '%s'", n)
		}
		l.Debug("remove %s", filepath.Join(dir, n))
	}
	return nil
}

func majmin(v string) string {
	if len(v) == 0 {
		return v
	}

	if v[0] != 'v' {
		v = "v" + v
	}

	return semver.MajorMinor(v)
}
