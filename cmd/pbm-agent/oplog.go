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
func (a *Agent) OplogReplay(
	ctx context.Context,
	r *ctrl.ReplayCmd,
	opID ctrl.OPID,
	ep config.Epoch,
) {
	if r == nil {
		log.Error(ctx, "missed command")
		return
	}

	log.Info(ctx, "time: %s-%s",
		time.Unix(int64(r.Start.T), 0).UTC().Format(time.RFC3339),
		time.Unix(int64(r.End.T), 0).UTC().Format(time.RFC3339))

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		log.Error(ctx, "get node info: %s", err.Error())
		return
	}
	if !nodeInfo.IsPrimary {
		log.Info(ctx, "node in not suitable for restore")
		return
	}
	if nodeInfo.ArbiterOnly {
		log.Debug(ctx, "arbiter node. skip")
		return
	}

	epoch := ep.TS()
	lck := lock.NewLock(a.leadConn, lock.LockHeader{
		Type:    ctrl.CmdReplay,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
		OPID:    opID.String(),
		Epoch:   &epoch,
	})

	nominated, err := a.acquireLock(ctx, lck)
	if err != nil {
		log.Error(ctx, "acquiring lock: %s", err.Error())
		return
	}
	if !nominated {
		log.Debug(ctx, "oplog replay: skip: lock not acquired")
		return
	}

	defer func() {
		if err := lck.Release(); err != nil {
			log.Error(ctx, "release lock: %s", err.Error())
		}
	}()

	cfg, err := config.GetConfig(ctx, a.leadConn)
	if err != nil {
		log.Error(ctx, "get PBM config: %v", err)
		return
	}

	log.Info(ctx, "oplog replay started")
	rr := restore.New(a.leadConn, a.nodeConn, a.brief, cfg, r.RSMap, 0)
	err = rr.ReplayOplog(ctx, r, opID)
	if err != nil {
		if errors.Is(err, restore.ErrNoDataForShard) {
			log.Info(ctx, "no oplog for the shard, skipping")
		} else {
			log.Error(ctx, "oplog replay: %v", err.Error())
		}
		return
	}
	log.Info(ctx, "oplog replay successfully finished")

	resetEpoch, err := config.ResetEpoch(ctx, a.leadConn)
	if err != nil {
		log.Error(ctx, "reset epoch: %s", err.Error())
		return
	}

	log.Debug(ctx, "epoch set to %v", resetEpoch)
}
