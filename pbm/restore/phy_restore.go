package restore

import (
	"context"
	"encoding/json"
	"io"
	slog "log"
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

	defaultPort = 27017
)

type PhysRestore struct {
	cn     *pbm.PBM
	node   *pbm.Node
	dbpath string
	port   int
	efport string
	rsConf *pbm.RSConfig

	name     string
	opid     string
	nodeInfo *pbm.NodeInfo
	stg      storage.Storage
	bcp      *pbm.BackupMeta
	// Shards to participate in restore. Num of shards in bcp could
	// be less than in the cluster and this is ok.
	//
	// Only the restore leader would have this info.
	shards []pbm.Shard

	stgpath string

	stopHB chan struct{}

	log *log.Event
}

func NewPhysical(cn *pbm.PBM, node *pbm.Node, role pbm.ReplsetRole) (*PhysRestore, error) {
	opts, err := node.GetOpts()
	if err != nil {
		return nil, errors.Wrap(err, "get mongo options")
	}
	p := opts.Storage.DBpath
	if p == "" {
		switch role {
		case pbm.RoleConfigSrv:
			p = defaultCSRSdbpath
		default:
			p = defaultRSdbpath
		}
	}
	if opts.Net.Port == 0 {
		opts.Net.Port = defaultPort
	}

	rcf, err := node.GetRSconf()
	if err != nil {
		return nil, errors.Wrap(err, "get replset config")
	}

	rscs, _ := json.Marshal(rcf)
	slog.Printf("RS_CONF:\n%s", rscs)
	slog.Printf("OPTS: %v", opts)

	return &PhysRestore{
		cn:     cn,
		node:   node,
		dbpath: p,
		rsConf: rcf,
		port:   opts.Net.Port,
		efport: strconv.Itoa(opts.Net.Port + 1111),
	}, nil
}

// Close releases object resources.
// Should be run to avoid leaks.
func (r *PhysRestore) Close() {
	if r.stopHB != nil {
		close(r.stopHB)
	}
}

func (r *PhysRestore) flush() error {
	err := r.node.Shutdown()
	if err != nil {
		return errors.Wrap(err, "shutdown server")
	}

	// !!!!!!!!
	time.Sleep(time.Second * 20)

	err = removeAll(r.dbpath)
	if err != nil {
		return errors.Wrapf(err, "flush dbpath %s", r.dbpath)
	}

	return nil
}

func (r *PhysRestore) Follower(cmd pbm.RestoreCmd, l *log.Event) error {
	r.log = l
	r.name = cmd.Name

	err := r.waitForStatus(pbm.StatusDumpDone)
	if err != nil {
		return errors.Wrap(err, "waiting for dumpDone")
	}

	err = r.flush()
	if err != nil {
		return err
	}

	return nil
}

func (r *PhysRestore) Snapshot(cmd pbm.RestoreCmd, opid pbm.OPID, l *log.Event) (err error) {
	defer func() {
		if err != nil && !errors.Is(err, ErrNoDatForShard) {
			ferr := r.MarkFailed(err)
			if ferr != nil {
				l.Error("mark restore as failed `%v`: %v", err, ferr)
			}
		}

		r.Close()
	}()

	err = r.init(cmd.Name, opid, l)
	if err != nil {
		return errors.Wrap(err, "init")
	}

	err = r.prepareBackup(cmd.BackupName)
	if err != nil {
		return err
	}

	err = r.cn.ChangeRestoreRSState(r.name, r.nodeInfo.SetName, pbm.StatusRunning, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDumpDone")
	}

	if r.nodeInfo.IsLeader() {
		err = r.reconcileStatus(pbm.StatusRunning, &pbm.WaitActionStart)
		if err != nil {
			if errors.Cause(err) == errConvergeTimeOut {
				return errors.Wrap(err, "couldn't get response from all shards")
			}
			return errors.Wrap(err, "check cluster for restore started")
		}
	}

	err = r.waitForStatus(pbm.StatusRunning)
	if err != nil {
		return errors.Wrap(err, "waiting for start")
	}

	l.Debug("flushing old data")
	err = r.flush()
	if err != nil {
		l.Error("FLUSH: %v", err)
		return err
	}

	err = r.copyFiles()
	if err != nil {
		return errors.Wrap(err, "copy files")
	}

	l.Debug("running stage 1")
	err = r.stage1()
	if err != nil {
		return errors.Wrap(err, "run stage_1")
	}

	l.Debug("running stage 2")
	err = r.stage2()
	if err != nil {
		return errors.Wrap(err, "run stage_2")
	}

	l.Debug("running stage 3")
	err = r.stage3()
	if err != nil {
		return errors.Wrap(err, "run stage_3")
	}

	err = r.cn.ChangeRestoreRSState(r.name, r.nodeInfo.SetName, pbm.StatusDumpDone, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDumpDone")
	}

	if r.nodeInfo.IsLeader() {
		err = r.reconcileStatus(pbm.StatusDumpDone, nil)
		if err != nil {
			if errors.Cause(err) == errConvergeTimeOut {
				return errors.Wrap(err, "couldn't get response from all shards")
			}
			return errors.Wrap(err, "check cluster for restore started")
		}
	}

	// !! pbmagent_rs101  | 2021-11-29T06:29:23.000+0000 E [restore/2021-11-29T06:26:43Z] waiting for dumpDone: restore stuck, last beat ts: 1638167330
	// ! need defer with hs and post errors
	err = r.waitForStatus(pbm.StatusDumpDone)
	if err != nil {
		return errors.Wrap(err, "waiting for dumpDone")
	}

	// l.Debug("running stage 4")
	// err = r.stage4()
	// if err != nil {
	// 	return errors.Wrap(err, "run stage_4")
	// }

	return nil
}

func (r *PhysRestore) copyFiles() error {
	for _, rs := range r.bcp.Replsets {
		if rs.Name == r.nodeInfo.SetName {
			for _, f := range rs.Files {
				src := filepath.Join(r.bcp.Name, r.nodeInfo.SetName, f.Name)
				dst := filepath.Join(r.dbpath, f.Name)

				err := os.MkdirAll(path.Dir(dst), os.ModeDir|0700)
				if err != nil {
					return errors.Wrapf(err, "create path %s", path.Dir(dst))
				}

				r.log.Info("copy <%s> to <%s>", src, dst)
				// copy files: copy file <2021-11-26T16:34:39Z/rs1/index-40-7668660307985647982.wt> to </data/db/index-40-7668660307985647982.wt>: create dst: open /opt/backups/data/db/index-40-7668660307985647982.wt: no such file or directory
				sr, err := r.stg.SourceReader(src)
				if err != nil {
					return errors.Wrapf(err, "create source reader for <%s>", src)
				}
				defer sr.Close()

				fw, err := os.Create(dst)
				if err != nil {
					return errors.Wrapf(err, "create destination file <%s>", dst)
				}
				defer fw.Close()
				err = os.Chmod(dst, f.Fmode)
				if err != nil {
					return errors.Wrapf(err, "change permissions for file <%s>", dst)
				}

				_, err = io.Copy(fw, sr)
				if err != nil {
					return errors.Wrapf(err, "copy file <%s>", dst)
				}
			}
		}
	}
	return nil
}

func (r *PhysRestore) stage1() error {
	err := startMongo("--dbpath", r.dbpath, "--port", r.efport, "--setParameter", "disableLogicalSessionCacheRefresh=true")
	if err != nil {
		return errors.Wrap(err, "start mongo")
	}

	c, err := conn(r.efport)
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

	_, err = c.Database("local").Collection("replset.oplogTruncateAfterPoint").InsertOne(ctx,
		bson.M{"_id": "oplogTruncateAfterPoint", "oplogTruncateAfterPoint": r.bcp.LastWriteTS},
	)
	if err != nil {
		return errors.Wrap(err, "set oplogTruncateAfterPoint")
	}

	// remove pbm locks -- no more need as database run on ephemeral port
	// if r.nodeInfo.IsLeader() {
	// 	_, err = c.Database(pbm.DB).Collection(pbm.LockCollection).DeleteMany(ctx, bson.D{})
	// 	if err != nil {
	// 		return errors.Wrap(err, "delete from pbm.LockCollection")
	// 	}
	// }

	err = shutdown(c)
	if err != nil {
		return errors.Wrap(err, "shutdown mongo")
	}

	return nil
}

func shutdown(c *mongo.Client) error {
	// n.cn.Database("admin").RunCommand(n.ctx, bson.D{{"shutdown", 1}, {"force", true}}).Err()
	err := c.Database("admin").RunCommand(context.Background(), bson.D{{"shutdown", 1}}).Err()
	if err == nil || strings.Contains(err.Error(), "socket was unexpectedly closed") {
		return nil
	}
	return err
}

func (r *PhysRestore) stage2() error {
	err := startMongo("--dbpath", r.dbpath, "--replSet", r.rsConf.ID, "--port", r.efport, "--setParameter", "disableLogicalSessionCacheRefresh=true")
	if err != nil {
		return errors.Wrap(err, "start mongo")
	}

	c, err := conn(r.efport)
	if err != nil {
		return errors.Wrap(err, "connect to mongo")
	}

	err = c.Database("admin").RunCommand(context.Background(),
		bson.D{{"replSetInitiate", pbm.RSConfig{
			ID:       r.rsConf.ID,
			CSRS:     false,
			Version:  1,
			Members:  []pbm.RSMember{{ID: 0, Host: "localhost:" + r.efport}},
			Settings: r.rsConf.Settings,
		}}}).Err()
	if err != nil {
		return errors.Wrap(err, "replSetInitiate")
	}

	err = shutdown(c)
	if err != nil {
		return errors.Wrap(err, "shutdown mongo")
	}

	return nil
}

func (r *PhysRestore) stage3() error {
	err := startMongo("--dbpath", r.dbpath, "--port", r.efport)
	if err != nil {
		return errors.Wrap(err, "start mongo")
	}

	c, err := conn(r.efport)
	if err != nil {
		return errors.Wrap(err, "connect to mongo")
	}

	ctx := context.Background()

	_, err = c.Database("admin").Collection("system.version").DeleteOne(ctx, bson.D{{"_id", "minOpTimeRecovery"}})
	if err != nil {
		return errors.Wrap(err, "delete minOpTimeRecovery from admin.system.version")
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

	// _, err = c.Database("local").Collection("system.replset").UpdateOne(ctx,
	// 	bson.D{{"_id", r.rsConf.ID}, {"members._id", 0}},
	// 	bson.D{{"$set", bson.M{"members.$.host": r.nodeInfo.Me}}},
	// )

	_, err = c.Database("local").Collection("system.replset").UpdateOne(ctx,
		bson.D{{"_id", r.rsConf.ID}},
		bson.D{{"$set", bson.M{"members": r.rsConf.Members}}},
	)
	if err != nil {
		return errors.Wrapf(err, "upate rs.member host to %s", r.nodeInfo.Me)
	}

	err = shutdown(c)
	if err != nil {
		return errors.Wrap(err, "shutdown mongo")
	}

	return nil
}

func (r *PhysRestore) stage4() error {
	err := startMongo("--dbpath", r.dbpath, "--replSet", r.rsConf.ID, "--port", r.efport)
	if err != nil {
		return errors.Wrap(err, "start mongo")
	}

	c, err := conn(r.efport)
	if err != nil {
		return errors.Wrap(err, "connect to mongo")
	}

	rscs, _ := json.Marshal(r.rsConf)
	slog.Printf("RS_CONF:\n%s", rscs)

	err = c.Database("admin").RunCommand(context.Background(), bson.D{{"replSetReconfig", r.rsConf}, {"force", true}}).Err()
	if err != nil {
		return errors.Wrap(err, "replSetReconfig")
	}

	err = shutdown(c)
	if err != nil {
		return errors.Wrap(err, "shutdown mongo")
	}

	return nil
}

func conn(port string) (*mongo.Client, error) {
	ctx := context.Background()

	conn, err := mongo.NewClient(options.Client().
		SetHosts([]string{"localhost:" + port}).
		SetAppName("pbm-physical-restore").
		SetDirect(true).
		SetConnectTimeout(time.Second * 60))
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
	return cmd.Start()
}

func (r *PhysRestore) init(name string, opid pbm.OPID, l *log.Event) (err error) {
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
			Type:     pbm.PhysicalBackup,
			OPID:     opid.String(),
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
	err = r.waitForStatus(pbm.StatusStarting)
	if err != nil {
		return errors.Wrap(err, "waiting for start")
	}

	r.stg, err = r.cn.GetStorage(r.log)
	if err != nil {
		return errors.Wrap(err, "get backup storage")
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
		return errors.Errorf("backup wasn't successful: status: %s, error: %s", r.bcp.Status, r.bcp.Error)
	}

	mgoV, err := r.node.GetMongoVersion()
	if err != nil || len(mgoV.Version) < 1 {
		return errors.Wrap(err, "define mongo version")
	}

	if semver.Compare(majmin(r.bcp.MongoVersion), majmin(mgoV.VersionString)) != 0 {
		return errors.Errorf("backup's Mongo version (%s) is not compatible with Mongo %s", r.bcp.PBMVersion, mgoV.VersionString)
	}

	if r.nodeInfo.IsLeader() {
		s, err := r.cn.ClusterMembers(r.nodeInfo)
		if err != nil {
			return errors.Wrap(err, "get cluster members")
		}

		fl := make(map[string]pbm.Shard, len(s))
		for _, rs := range s {
			fl[rs.RS] = rs
		}

		var nors []string
		for _, sh := range r.bcp.Replsets {
			rs, ok := fl[sh.Name]
			if !ok {
				nors = append(nors, sh.Name)
				continue
			}

			r.shards = append(r.shards, rs)
		}

		if len(nors) > 0 {
			return errors.Errorf("extra/unknown replica set found in the backup: %s", strings.Join(nors, ","))
		}
	}

	var ok bool
	for _, v := range r.bcp.Replsets {
		if v.Name == r.nodeInfo.SetName {
			r.stgpath = filepath.Join(r.bcp.Name, r.nodeInfo.SetName)
			ok = true
			break
		}
	}
	if !ok {
		if r.nodeInfo.IsLeader() {
			return errors.New("no data for the config server or sole rs in backup")
		}
		return ErrNoDatForShard
	}

	// _, err = r.stg.FileStat(r.dumpFile)
	// if err != nil {
	// 	return errors.Errorf("failed to ensure snapshot file %s: %v", r.dumpFile, err)
	// }
	// _, err = r.stg.FileStat(r.oplogFile)
	// if err != nil {
	// 	return errors.Errorf("failed to ensure oplog file %s: %v", r.oplogFile, err)
	// }

	rsMeta := pbm.RestoreReplset{
		Name:       r.nodeInfo.SetName,
		StartTS:    time.Now().UTC().Unix(),
		Status:     pbm.StatusRunning,
		Conditions: []pbm.Condition{},
	}

	err = r.cn.AddRestoreRSMeta(r.name, rsMeta)
	if err != nil {
		return errors.Wrap(err, "add shard's metadata")
	}

	return nil
}

// MarkFailed sets the restore and rs state as failed with the given message
func (r *PhysRestore) MarkFailed(e error) error {
	err := r.cn.ChangeRestoreState(r.name, pbm.StatusError, e.Error())
	if err != nil {
		return errors.Wrap(err, "set backup state")
	}
	err = r.cn.ChangeRestoreRSState(r.name, r.nodeInfo.SetName, pbm.StatusError, e.Error())
	return errors.Wrap(err, "set replset state")
}

func (r *PhysRestore) waitForStatus(status pbm.Status) error {
	r.log.Debug("waiting for '%s' status", status)
	return waitForStatus(r.cn, r.name, status)
}

// func (r *PhysRestore) waitRSStatus(status pbm.Status) error {
// 	r.log.Debug("waiting for RS status  '%s'", status)
// 	return waitRSStatus(r.cn, r.name, r.rsConf.ID, status)
// }

// func waitRSStatus(cn *pbm.PBM, restore, rs string, status pbm.Status) error {
// 	tk := time.NewTicker(time.Second * 1)
// 	defer tk.Stop()
// 	for {
// 		select {
// 		case <-tk.C:
// 			meta, err := cn.GetRestoreMeta(restore)
// 			if errors.Is(err, pbm.ErrNotFound) {
// 				continue
// 			}
// 			if err != nil {
// 				return errors.Wrap(err, "get restore metadata")
// 			}

// 			clusterTime, err := cn.ClusterTime()
// 			if err != nil {
// 				return errors.Wrap(err, "read cluster time")
// 			}

// 			if meta.Hb.T+pbm.StaleFrameSec < clusterTime.T {
// 				return errors.Errorf("restore stuck, last beat ts: %d", meta.Hb.T)
// 			}

// 			switch meta.Status {
// 			case status:
// 				return nil
// 			case pbm.StatusError:
// 				return errors.Errorf("cluster failed: %s", meta.Error)
// 			}
// 		case <-cn.Context().Done():
// 			return nil
// 		}
// 	}
// }

func (r *PhysRestore) reconcileStatus(status pbm.Status, timeout *time.Duration) error {
	if timeout != nil {
		return errors.Wrap(convergeClusterWithTimeout(r.cn, r.name, r.opid, r.shards, status, *timeout), "convergeClusterWithTimeout")
	}
	return errors.Wrap(convergeCluster(r.cn, r.name, r.opid, r.shards, status), "convergeCluster")
}

func removeAll(dir string) error {
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
		slog.Println("RM", filepath.Join(dir, n))
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
