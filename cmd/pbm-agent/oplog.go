package main

import (
	"context"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
)

// OplogReplay replays oplog between r.Start and r.End timestamps (wall time in UTC tz)
func (a *Agent) OplogReplay(ctx context.Context, r *ctrl.ReplayCmd, opID ctrl.OPID, ep config.Epoch) {
	logger := log.FromContext(ctx)

	if r == nil {
		l := logger.NewEvent(string(ctrl.CmdReplay), "", opID.String(), ep.TS())
		l.Error("oplog replay: missed command")
		return
	}

	l := logger.NewEvent(string(ctrl.CmdReplay), r.Name, opID.String(), ep.TS())
	ctx = log.SetLogEventToContext(ctx, l)
	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)

	if err != nil {
		l.Error("oplog replay: et node info: %s", err.Error())
		return
	}
	if !nodeInfo.IsPrimary {
		l.Debug("oplog replay: node is not primary, check pbm agent log on primary node for oplog replay details")
		return
	}
	if nodeInfo.ArbiterOnly {
		l.Debug("oplog replay: node is arbiter only, check pbm agent log on primary node for oplog replay details")
		return
	}

	startTime := time.Unix(int64(r.Start.T), 0).UTC()
	endTime := time.Unix(int64(r.End.T), 0).UTC()
	l.Info("oplog replay: starting oplog replay for period: %s - %s, duration: %s",
		startTime.Format(time.RFC3339),
		endTime.Format(time.RFC3339),
		endTime.Sub(startTime),
	)

	epoch := ep.TS()
	lck := lock.NewLock(a.leadConn, lock.LockHeader{
		Type:    ctrl.CmdReplay,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
		OPID:    opID.String(),
		Epoch:   &epoch,
	})

	nominated, err := a.acquireLock(ctx, lck, l)
	if err != nil {
		l.Error("oplog replay: acquiring lock: %s", err.Error())
		return
	}
	if !nominated {
		l.Debug("oplog replay: skip: lock not acquired")
		return
	}

	defer func() {
		if err := lck.Release(); err != nil {
			l.Error("oplog replay: release lock: %s", err.Error())
		}
	}()

	cfg, err := config.GetConfig(ctx, a.leadConn)
	if err != nil {
		l.Error("oplog replay: get PBM config: %v", err)
		return
	}

	l.Info("oplog replay: started")
	rr := restore.New(a.leadConn, a.nodeConn, a.brief, cfg, r.RSMap, 0, 1)
	err = rr.ReplayOplog(ctx, r, opID, l)
	if err != nil {
		if errors.Is(err, restore.ErrNoDataForShard) {
			l.Info("oplog replay: no oplog for the shard, skipping")
		} else {
			l.Error("oplog replay: %v", err.Error())
		}
		return
	}
	l.Info("oplog replay: successfully finished")

	resetEpoch, err := config.ResetEpoch(ctx, a.leadConn)
	if err != nil {
		l.Error("oplog replay: reset epoch: %s", err.Error())
		return
	}

	l.Debug("oplog replay: epoch set to %v", resetEpoch)
}
