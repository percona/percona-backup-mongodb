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
		l.Error("missed command")
		return
	}

	l := logger.NewEvent(string(ctrl.CmdReplay), r.Name, opID.String(), ep.TS())
	ctx = log.SetLogEventToContext(ctx, l)
<<<<<<< HEAD
=======

	startTime := time.Unix(int64(r.Start.T), 0).UTC()
	endTime := time.Unix(int64(r.End.T), 0).UTC()
	l.Info("Starting oplog replay operation",
		"start_ts", startTime,
		"end_ts", endTime,
		"oplog_range_seconds", endTime.Sub(startTime).Seconds(),
	)

>>>>>>> c28deb23 (PBM-1673 Messages during oplog replay are confusing)
	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		l.Error("get node info: %s", err.Error())
		return
	}
	if !nodeInfo.IsPrimary {
		l.Debug("node is not primary, check pbm agent log on primary node for oplog replay details")
		return
	}
	if nodeInfo.ArbiterOnly {
		l.Debug("node is arbiter only, check pbm agent log on primary node for oplog replay details")
		return
	}

	startTime := time.Unix(int64(r.Start.T), 0).UTC()
	endTime := time.Unix(int64(r.End.T), 0).UTC()
	l.Info("starting oplog replay for period: %s - %s, duration: %s",
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
		l.Error("acquiring lock: %s", err.Error())
		return
	}
	if !nominated {
		l.Debug("skip: lock not acquired")
		return
	}

	defer func() {
		if err := lck.Release(); err != nil {
			l.Error("release lock: %s", err.Error())
		}
	}()

	cfg, err := config.GetConfig(ctx, a.leadConn)
	if err != nil {
		l.Error("get PBM config: %v", err)
		return
	}

	l.Info("oplog replay started")
	rr := restore.New(a.leadConn, a.nodeConn, a.brief, cfg, r.RSMap, 0, 1)
	err = rr.ReplayOplog(ctx, r, opID, l)
	if err != nil {
		if errors.Is(err, restore.ErrNoDataForShard) {
			l.Info("no oplog for the shard, skipping")
		} else {
			l.Error("oplog replay error: %v", err.Error())
		}
		return
	}
	l.Info("oplog replay successfully finished")

	resetEpoch, err := config.ResetEpoch(ctx, a.leadConn)
	if err != nil {
		l.Error("reset epoch: %s", err.Error())
		return
	}

	l.Debug("epoch set to %v", resetEpoch)
}
