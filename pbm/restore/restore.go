package restore

import (
	"encoding/json"
	"time"

	mlog "github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/v2/pbm"
	"github.com/percona/percona-backup-mongodb/v2/pbm/log"
	"github.com/percona/percona-backup-mongodb/v2/pbm/storage"
)

func init() {
	// set date format for mongo tools (mongodump/mongorestore) logger
	//
	// duplicated in backup/restore packages just
	// in the sake of clarity
	mlog.SetDateFormat(log.LogTimeFormat)
	mlog.SetVerbosity(&options.Verbosity{
		VLevel: mlog.DebugLow,
	})
}

func GetMetaFromStore(stg storage.Storage, bcpName string) (*pbm.BackupMeta, error) {
	rd, err := stg.SourceReader(bcpName + pbm.MetadataFileSuffix)
	if err != nil {
		return nil, errors.Wrap(err, "get from store")
	}
	defer rd.Close()

	b := &pbm.BackupMeta{}
	err = json.NewDecoder(rd).Decode(b)

	return b, errors.Wrap(err, "decode")
}

func toState(cn *pbm.PBM, status pbm.Status, bcp string, inf *pbm.NodeInfo, reconcileFn reconcileStatus, wait *time.Duration) (meta *pbm.RestoreMeta, err error) {
	err = cn.ChangeRestoreRSState(bcp, inf.SetName, status, "")
	if err != nil {
		return nil, errors.Wrap(err, "set shard's status")
	}

	if inf.IsLeader() {
		meta, err = reconcileFn(status, wait)
		if err != nil {
			if errors.Cause(err) == errConvergeTimeOut {
				return nil, errors.Wrap(err, "couldn't get response from all shards")
			}
			return nil, errors.Wrapf(err, "check cluster for restore `%s`", status)
		}
	}

	err = waitForStatus(cn, bcp, status)
	if err != nil {
		return nil, errors.Wrapf(err, "waiting for %s", status)
	}

	return meta, nil
}

type reconcileStatus func(status pbm.Status, timeout *time.Duration) (*pbm.RestoreMeta, error)

// convergeCluster waits until all participating shards reached `status` and updates a cluster status
func convergeCluster(cn *pbm.PBM, name, opid string, shards []pbm.Shard, status pbm.Status) (*pbm.RestoreMeta, error) {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			ok, meta, err := converged(cn, name, opid, shards, status)
			if err != nil {
				return meta, err
			}
			if ok {
				return meta, nil
			}
		case <-cn.Context().Done():
			return nil, nil
		}
	}
}

var errConvergeTimeOut = errors.New("reached converge timeout")

// convergeClusterWithTimeout waits up to the geiven timeout until all participating shards reached
// `status` and then updates the cluster status
func convergeClusterWithTimeout(cn *pbm.PBM, name, opid string, shards []pbm.Shard, status pbm.Status, t time.Duration) (meta *pbm.RestoreMeta, err error) {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	tout := time.NewTicker(t)
	defer tout.Stop()
	for {
		select {
		case <-tk.C:
			var ok bool
			ok, meta, err = converged(cn, name, opid, shards, status)
			if err != nil {
				return meta, err
			}
			if ok {
				return meta, nil
			}
		case <-tout.C:
			return meta, errConvergeTimeOut
		case <-cn.Context().Done():
			return nil, nil
		}
	}
}

func converged(cn *pbm.PBM, name, opid string, shards []pbm.Shard, status pbm.Status) (bool, *pbm.RestoreMeta, error) {
	shardsToFinish := len(shards)
	bmeta, err := cn.GetRestoreMeta(name)
	if err != nil {
		return false, nil, errors.Wrap(err, "get backup metadata")
	}

	clusterTime, err := cn.ClusterTime()
	if err != nil {
		return false, nil, errors.Wrap(err, "read cluster time")
	}

	for _, sh := range shards {
		for _, shard := range bmeta.Replsets {
			if shard.Name == sh.RS {
				// check if node alive
				lock, err := cn.GetLockData(&pbm.LockHeader{
					Type:    pbm.CmdRestore,
					OPID:    opid,
					Replset: shard.Name,
				})

				// nodes are cleaning its locks moving to the done status
				// so no lock is ok and not need to ckech the heartbeats
				if status != pbm.StatusDone && err != mongo.ErrNoDocuments {
					if err != nil {
						return false, nil, errors.Wrapf(err, "unable to read lock for shard %s", shard.Name)
					}
					if lock.Heartbeat.T+pbm.StaleFrameSec < clusterTime.T {
						return false, nil, errors.Errorf("lost shard %s, last beat ts: %d", shard.Name, lock.Heartbeat.T)
					}
				}

				// check status
				switch shard.Status {
				case status:
					shardsToFinish--
				case pbm.StatusError:
					bmeta.Status = pbm.StatusError
					bmeta.Error = shard.Error
					return false, nil, errors.Errorf("restore on the shard %s failed with: %s", shard.Name, shard.Error)
				}
			}
		}
	}

	if shardsToFinish == 0 {
		err := cn.ChangeRestoreState(name, status, "")
		if err != nil {
			return false, nil, errors.Wrapf(err, "update backup meta with %s", status)
		}
		return true, bmeta, nil
	}

	return false, bmeta, nil
}

func waitForStatus(cn *pbm.PBM, name string, status pbm.Status) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			meta, err := cn.GetRestoreMeta(name)
			if errors.Is(err, pbm.ErrNotFound) {
				continue
			}
			if err != nil {
				return errors.Wrap(err, "get restore metadata")
			}

			clusterTime, err := cn.ClusterTime()
			if err != nil {
				return errors.Wrap(err, "read cluster time")
			}

			if meta.Hb.T+pbm.StaleFrameSec < clusterTime.T {
				return errors.Errorf("restore stuck, last beat ts: %d", meta.Hb.T)
			}

			switch meta.Status {
			case status:
				return nil
			case pbm.StatusError:
				return errors.Errorf("cluster failed: %s", meta.Error)
			}
		case <-cn.Context().Done():
			return nil
		}
	}
}

func waitForStatusT(cn *pbm.PBM, name string, status pbm.Status, t time.Duration) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	tout := time.NewTicker(t)
	defer tout.Stop()
	for {
		select {
		case <-tk.C:
			meta, err := cn.GetRestoreMeta(name)
			if errors.Is(err, pbm.ErrNotFound) {
				continue
			}
			if err != nil {
				return errors.Wrap(err, "get restore metadata")
			}

			clusterTime, err := cn.ClusterTime()
			if err != nil {
				return errors.Wrap(err, "read cluster time")
			}

			if meta.Hb.T+pbm.StaleFrameSec < clusterTime.T {
				return errors.Errorf("restore stuck, last beat ts: %d", meta.Hb.T)
			}

			switch meta.Status {
			case status:
				return nil
			case pbm.StatusError:
				return errors.Errorf("cluster failed: %s", meta.Error)
			}
		case <-tout.C:
			return errConvergeTimeOut
		case <-cn.Context().Done():
			return nil
		}
	}
}
