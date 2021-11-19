package restore

import (
	"context"
	"os"
	"os/exec"
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

	return &PhysRestore{
		cn:     cn,
		node:   node,
		dbpath: p,
		rsConf: rcf,
		port:   opts.Net.Port,
		efport: strconv.Itoa(opts.Net.Port + 1111),
	}, nil
}

func (r *PhysRestore) Flush() error {
	err := r.node.Shutdown()
	if err != nil {
		return errors.Wrap(err, "shutdown server")
	}

	err = removeAll(r.dbpath)
	if err != nil {
		return errors.Wrapf(err, "flush dbpath %s", r.dbpath)
	}

	return nil
}

func (r *PhysRestore) Follower(bcp string) error {
	// - wait for `init`
	// - flush & shutdown
	// - set in metadata.followers["name"] = {ts, ready}
	return nil
}

func (r *PhysRestore) Snapshot(cmd pbm.RestoreCmd, opid pbm.OPID, l *log.Event) error {
	err := r.init(cmd.Name, opid, l)
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

	err = r.Flush()
	if err != nil {
		return err
	}

	err = startMongo("--dbpath", r.dbpath, "--port", r.efport, "--setParameter", "disableLogicalSessionCacheRefresh=true")
	if err != nil {
		return errors.Wrap(err, "start mongo phase 1")
	}

	c, err := conn(r.efport)
	if err != nil {
		return errors.Wrap(err, "connect to mongo phase 1")
	}

	ctx := context.Background()

	// err := n.cn.Database(DB).RunCommand(n.ctx, bson.D{{"isMaster", 1}}).Decode(i)
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

	err = c.Database("admin").RunCommand(ctx, bson.D{{"shutdown", 1}, {"force", true}}).Err()
	if err != nil {
		return errors.Wrap(err, "shutdown mongo phase1")
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
		r.bcp, err = getMetaFromStore(r.stg, backupName)
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

func (r *PhysRestore) waitForStatus(status pbm.Status) error {
	r.log.Debug("waiting for '%s' status", status)
	return waitForStatus(r.cn, r.name, status)
}

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
