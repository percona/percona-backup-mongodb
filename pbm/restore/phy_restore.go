package restore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

	mongofslock = "mongod.lock"

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
	if opts.Net.Port == 0 {
		opts.Net.Port = defaultPort
	}

	rcf, err := node.GetRSconf()
	if err != nil {
		return nil, errors.Wrap(err, "get replset config")
	}

	if inf.SetName == "" {
		return nil, errors.New("undefined replica set")
	}

	return &PhysRestore{
		cn:       cn,
		node:     node,
		dbpath:   p,
		rsConf:   rcf,
		nodeInfo: inf,
		port:     opts.Net.Port,
		efport:   strconv.Itoa(opts.Net.Port + 1111),
	}, nil
}

// Close releases object resources.
// Should be run to avoid leaks.
func (r *PhysRestore) close() {
	if r.stopHB != nil {
		if _, ok := <-r.stopHB; !ok {
			return
		}
		close(r.stopHB)
	}
}

func (r *PhysRestore) flush() error {
	r.log.Debug("shutdown server")

	for {
		inf, err := r.node.GetInfo()
		if err != nil {
			return errors.Wrap(err, "get node info")
		}
		if !inf.IsPrimary {
			err = r.node.Shutdown()
			if err != nil {
				return errors.Wrap(err, "shutdown server")
			}
			break
		}
		r.log.Debug("waiting to became secondary")
		time.Sleep(time.Second * 1)
	}

	err := r.waitMgoShutdown()
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

func (r *PhysRestore) toState(status pbm.Status) error {
	r.log.Info("moving to state %s", status)

	err := r.stg.Save(fmt.Sprintf("%s/%s/%s.%s.%s", pbm.PhysRestoresDir, r.name, r.rsConf.ID, r.nodeInfo.Me, status),
		bytes.NewReader([]byte{'1'}), 1)
	if err != nil {
		return errors.Wrap(err, "write node state")
	}

	if r.nodeInfo.IsPrimary {
		var flwrs []string
		for _, m := range r.rsConf.Members {
			flwrs = append(flwrs, fmt.Sprintf("%s/%s/%s.%s.%s", pbm.PhysRestoresDir, r.name, r.rsConf.ID, m.Host, status))
		}

		r.log.Info("waiting for nodes in rs %v", flwrs)
		err = r.waitFiles(flwrs...)
		if err != nil {
			return errors.Wrap(err, "waite for nodes in rs")
		}

		err = r.stg.Save(fmt.Sprintf("%s/%s/%s.%s", pbm.PhysRestoresDir, r.name, r.rsConf.ID, status),
			bytes.NewReader([]byte{'1'}), 1)
		if err != nil {
			return errors.Wrap(err, "write replset state")
		}
	}

	if r.nodeInfo.IsClusterLeader() {
		var shrds []string
		for _, s := range r.shards {
			shrds = append(shrds, fmt.Sprintf("%s/%s/%s.%s", pbm.PhysRestoresDir, r.name, s.RS, status))
		}

		r.log.Info("waiting for shards %v", shrds)
		err = r.waitFiles(shrds...)
		if err != nil {
			return errors.Wrap(err, "wait for shards")
		}

		err = r.stg.Save(fmt.Sprintf("%s/%s/cluster.%s", pbm.PhysRestoresDir, r.name, status),
			bytes.NewReader([]byte{'1'}), 1)
		if err != nil {
			return errors.Wrap(err, "write replset state")
		}
	}

	r.log.Info("waiting for cluster")
	err = r.waitFiles(fmt.Sprintf("%s/%s/cluster.%s", pbm.PhysRestoresDir, r.name, status))
	if err != nil {
		return errors.Wrap(err, "wait for shards")
	}

	r.log.Debug("converged to state %s", status)

	return nil
}

func (r *PhysRestore) waitFiles(n ...string) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for range tk.C {
		n2 := append([]string{}, n...)
		for i := range n2 {
			_, err := r.stg.FileStat(n2[i])
			if err == storage.ErrNotExist {
				continue
			}

			if err != nil && err != storage.ErrEmpty {
				return errors.Wrapf(err, "get file %s", n[i])
			}

			n = append(n2[:i], n2[i+1:]...)
		}

		if len(n) == 0 {
			return nil
		}
	}

	return storage.ErrNotExist
}

func (r *PhysRestore) Snapshot(cmd pbm.RestoreCmd, opid pbm.OPID, l *log.Event) (err error) {
	meta := &pbm.RestoreMeta{
		Type: pbm.PhysicalBackup,
		OPID: opid.String(),
		Name: r.name,
	}

	defer func() {
		if err != nil {
			if !errors.Is(err, ErrNoDatForShard) {
				ferr := r.MarkFailed(err)
				if ferr != nil {
					l.Error("mark restore as failed `%v`: %v", err, ferr)
				}
			}

			if r.nodeInfo.IsClusterLeader() && meta != nil {
				ferr := r.dumpMeta(meta, pbm.StatusError, err.Error())
				if ferr != nil {
					l.Error("write restore meta to storage `%v`: %v", err, ferr)
				}
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

	err = r.cn.ChangeRestoreRSState(r.name, r.nodeInfo.SetName, pbm.StatusRunning, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusRunning")
	}

	if r.nodeInfo.IsLeader() {
		meta, err = r.reconcileStatus(pbm.StatusRunning, &pbm.WaitActionStart)
		if err != nil {
			if errors.Cause(err) == errConvergeTimeOut {
				return errors.Wrap(err, "couldn't get response from all shards")
			}
			return errors.Wrap(err, "check cluster for restore started")
		}
	}

	err = r.waitForStatus(pbm.StatusRunning, &pbm.WaitActionStart)
	if err != nil {
		return errors.Wrap(err, "waiting for start")
	}

	// not to spam logs with failed hb attempts
	if r.stopHB != nil {
		r.stopHB <- struct{}{}
	}

	// don't write logs to the mongo anymore
	r.cn.Logger().PauseMgo()

	err = r.toState(pbm.StatusRunning)
	if err != nil {
		return errors.Wrapf(err, "moving to state %s", pbm.StatusRunning)
	}

	l.Info("flushing old data")
	err = r.flush()
	if err != nil {
		return err
	}

	l.Info("copying backup data")
	err = r.copyFiles()
	if err != nil {
		return errors.Wrap(err, "copy files")
	}

	l.Info("setting restore timestamp")
	err = r.stage1()
	if err != nil {
		return errors.Wrap(err, "set restore timestamp")
	}

	l.Info("recovering oplog as standalone")
	err = r.recoverStandalone()
	if err != nil {
		return errors.Wrap(err, "recover oplog as standalone")
	}

	l.Info("clean-up and reset replicaset config")
	err = r.stage3()
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
	ts := time.Now().Unix()

	meta.Status = s
	meta.Conditions = append(meta.Conditions, pbm.Condition{Timestamp: ts, Status: s})
	meta.LastTransitionTS = ts
	meta.Error = msg

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "\t")
	err := enc.Encode(meta)
	if err != nil {
		return errors.Wrap(err, "encode restore meta")
	}
	err = r.stg.Save(fmt.Sprintf("%s/%s.json", pbm.PhysRestoresDir, meta.Name), &buf, buf.Len())
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

				err := os.MkdirAll(filepath.Dir(dst), os.ModeDir|0700)
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

func (r *PhysRestore) stage1() error {
	err := startMongo("--dbpath", r.dbpath, "--port", r.efport, "--setParameter", "disableLogicalSessionCacheRefresh=true")
	if err != nil {
		return errors.Wrap(err, "start mongo")
	}

	c, err := conn(r.efport, "")
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

	_, err = c.Database("local").Collection("system.replset").InsertOne(ctx,
		pbm.RSConfig{
			ID:      r.rsConf.ID,
			CSRS:    r.nodeInfo.IsConfigSrv(),
			Version: 1,
			Members: []pbm.RSMember{{ID: 0, Host: "localhost:" + r.efport}},
			// Settings: r.rsConf.Settings,
		},
	)

	if err != nil {
		return errors.Wrapf(err, "upate rs.member host to %s", r.nodeInfo.Me)
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
	err := startMongo("--dbpath", r.dbpath, "--port", r.efport,
		"--setParameter", "recoverFromOplogAsStandalone=true",
		"--setParameter", "takeUnstableCheckpointOnShutdown=true")
	if err != nil {
		return errors.Wrap(err, "start mongo")
	}

	c, err := conn(r.efport, "")
	if err != nil {
		return errors.Wrap(err, "connect to mongo")
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

	c, err := conn(r.efport, "")
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

	// _, err = c.Database("admin").Collection("system.version").DeleteOne(ctx, bson.D{{"_id", "minOpTimeRecovery"}})
	// if err != nil {
	// 	return errors.Wrap(err, "delete minOpTimeRecovery from admin.system.version")
	// }
	// err = c.Database("config").Collection("cache.collections").Drop(ctx)
	// if err != nil {
	// 	return errors.Wrap(err, "drop config.cache.collections")
	// }
	// err = c.Database("config").Collection("cache.databases").Drop(ctx)
	// if err != nil {
	// 	return errors.Wrap(err, "drop config.cache.databases")
	// }
	// err = c.Database("config").Collection("cache.chunks.config.system.sessions").Drop(ctx)
	// if err != nil {
	// 	return errors.Wrap(err, "drop config.cache.chunks.config.system.sessions")
	// }

	_, err = c.Database("local").Collection("system.replset").DeleteMany(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "delete from system.replset")
	}

	// if r.nodeInfo.IsPrimary {
	// _, err = c.Database("local").Collection("system.replset").UpdateOne(ctx,
	// 	bson.D{{"_id", r.rsConf.ID}},
	// 	bson.D{{"$set", bson.M{"members": r.rsConf.Members}}},
	// )
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
	// }
	// else {
	// 	_, err = c.Database("local").Collection("system.replset").DeleteMany(ctx, bson.D{})
	// 	if err != nil {
	// 		return errors.Wrap(err, "delete from system.replset")
	// 	}
	// }

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

func (r *PhysRestore) init(name string, opid pbm.OPID, l *log.Event) (err error) {
	r.log = l

	r.name = name
	r.opid = opid.String()
	if r.nodeInfo.IsClusterLeader() {
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
			Leader:   r.nodeInfo.Me + "/" + r.rsConf.ID,
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
	err = r.waitForStatus(pbm.StatusStarting, &pbm.WaitActionStart)
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

	if !r.nodeInfo.IsPrimary {
		return nil
	}

	if r.nodeInfo.IsClusterLeader() {
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

func (r *PhysRestore) waitForStatus(status pbm.Status, tout *time.Duration) error {
	r.log.Debug("waiting for '%s' status", status)
	if tout != nil {
		return waitForStatusT(r.cn, r.name, status, *tout)
	}
	return waitForStatus(r.cn, r.name, status)
}

func (r *PhysRestore) reconcileStatus(status pbm.Status, timeout *time.Duration) (*pbm.RestoreMeta, error) {
	if timeout != nil {
		m, err := convergeClusterWithTimeout(r.cn, r.name, r.opid, r.shards, status, *timeout)
		return m, errors.Wrap(err, "convergeClusterWithTimeout")
	}
	m, err := convergeCluster(r.cn, r.name, r.opid, r.shards, status)
	return m, errors.Wrap(err, "convergeCluster")
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
		slog.Printf("remove %s", filepath.Join(dir, n))
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
