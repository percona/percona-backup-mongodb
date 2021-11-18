package restore

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/mod/semver"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

const (
	defaultRSdbpath   = "/data/db"
	defaultCSRSdbpath = "/data/configdb"
)

type PhysRestore struct {
	cn     *pbm.PBM
	node   *pbm.Node
	dbpath string
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
	// db.adminCommand( { getCmdLineOpts: 1  } )
	p, err := node.GetDBpath()
	if err != nil {
		return nil, errors.Wrap(err, "get dbPath")
	}
	if p == "" {
		switch role {
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

	return &PhysRestore{
		cn:     cn,
		node:   node,
		dbpath: p,
		rsConf: rcf,
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

	err = r.PrepareBackup(cmd.BackupName)
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

	return nil
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

func (r *PhysRestore) PrepareBackup(backupName string) (err error) {
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
			r.stgpath = r.nodeInfo.SetName
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
