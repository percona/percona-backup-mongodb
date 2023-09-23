package main

import (
	"time"

	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/lock"
	"github.com/percona/percona-backup-mongodb/internal/topo"
	"github.com/percona/percona-backup-mongodb/internal/types"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
)

// OplogReplay replays oplog between r.Start and r.End timestamps (wall time in UTC tz)
func (a *Agent) OplogReplay(ctx context.Context, r *types.ReplayCmd, opID types.OPID, ep config.Epoch) {
	if r == nil {
		l := a.log.NewEvent(string(defs.CmdReplay), "", opID.String(), ep.TS())
		l.Error("missed command")
		return
	}

	l := a.log.NewEvent(string(defs.CmdReplay), r.Name, opID.String(), ep.TS())

	l.Info("time: %s-%s",
		time.Unix(int64(r.Start.T), 0).UTC().Format(time.RFC3339),
		time.Unix(int64(r.End.T), 0).UTC().Format(time.RFC3339),
	)

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.node.Session())
	if err != nil {
		l.Error("get node info: %s", err.Error())
		return
	}
	if !nodeInfo.IsPrimary {
		l.Info("node in not suitable for restore")
		return
	}

	epoch := ep.TS()
	lck := lock.NewLock(a.pbm.Conn, lock.LockHeader{
		Type:    defs.CmdReplay,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
		OPID:    opID.String(),
		Epoch:   &epoch,
	})

	nominated, err := a.acquireLock(ctx, lck, l, nil)
	if err != nil {
		l.Error("acquiring lock: %s", err.Error())
		return
	}
	if !nominated {
		l.Debug("oplog replay: skip: lock not acquired")
		return
	}

	defer func() {
		if err := lck.Release(ctx); err != nil {
			l.Error("release lock: %s", err.Error())
		}
	}()

	l.Info("oplog replay started")
	if err := restore.New(a.pbm, a.node, r.RSMap).ReplayOplog(ctx, r, opID, l); err != nil {
		if errors.Is(err, restore.ErrNoDataForShard) {
			l.Info("no oplog for the shard, skipping")
		} else {
			l.Error("oplog replay: %v", err.Error())
		}
		return
	}
	l.Info("oplog replay successfully finished")

	resetEpoch, err := config.ResetEpoch(ctx, a.pbm.Conn)
	if err != nil {
		l.Error("reset epoch: %s", err.Error())
		return
	}

	l.Debug("epoch set to %v", resetEpoch)
}
