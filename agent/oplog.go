package agent

import (
	"fmt"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
	"github.com/pkg/errors"
)

// OplogReplay replays oplog between r.Start and r.End timestamps (wall time in UTC tz)
func (a *Agent) OplogReplay(r pbm.ReplayCmd, opID pbm.OPID, ep pbm.Epoch) {
	l := a.log.NewEvent(string(pbm.CmdReplay),
		fmt.Sprintf("%s-%s",
			time.Unix(int64(r.Start.T), 0).UTC().Format(time.RFC3339),
			time.Unix(int64(r.End.T), 0).UTC().Format(time.RFC3339)),
		opID.String(),
		ep.TS())

	nodeInfo, err := a.node.GetInfo()
	if err != nil {
		l.Error("get node info: %s", err.Error())
		return
	}
	if !nodeInfo.IsPrimary {
		l.Info("node in not suitable for restore")
		return
	}

	epoch := ep.TS()
	lock := a.pbm.NewLock(pbm.LockHeader{
		Type:    pbm.CmdReplay,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
		OPID:    opID.String(),
		Epoch:   &epoch,
	})

	nominated, err := a.acquireLock(lock, l, nil)
	if err != nil {
		l.Error("acquiring lock: %s", err.Error())
		return
	}
	if !nominated {
		l.Debug("oplog replay: skip: lock not acquired")
		return
	}

	defer func() {
		if err := lock.Release(); err != nil {
			l.Error("release lock: %s", err.Error())
		}
	}()

	l.Info("oplog replay started")
	if err := restore.New(a.pbm, a.node, r.RSMap).ReplayOplog(r, opID, l); err != nil {
		if errors.Is(err, restore.ErrNoDataForShard) {
			l.Info("no oplog for the shard, skipping")
		} else {
			l.Error("oplog replay: %v", err.Error())
		}
		return
	}
	l.Info("oplog replay successfully finished")

	resetEpoch, err := a.pbm.ResetEpoch()
	if err != nil {
		l.Error("reset epoch: %s", err.Error())
		return
	}

	l.Debug("epoch set to %v", resetEpoch)
}
