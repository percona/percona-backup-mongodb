package main

import (
	"context"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/util"
)

func (a *Agent) Restore(ctx context.Context, r *ctrl.RestoreCmd, opid ctrl.OPID, ep config.Epoch) {
	logger := log.FromContext(ctx)
	if r == nil {
		l := logger.NewEvent(string(ctrl.CmdRestore), "", opid.String(), ep.TS())
		l.Error("missed command")
		return
	}

	l := logger.NewEvent(string(ctrl.CmdRestore), r.Name, opid.String(), ep.TS())
	ctx = log.SetLogEventToContext(ctx, l)

	if !r.OplogTS.IsZero() {
		l.Info("to time: %s", time.Unix(int64(r.OplogTS.T), 0).UTC().Format(time.RFC3339))
	}

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		l.Error("get node info: %v", err)
		return
	}

	var lck *lock.Lock
	if nodeInfo.IsPrimary {
		epts := ep.TS()
		lck = lock.NewLock(a.leadConn, lock.LockHeader{
			Type:    ctrl.CmdRestore,
			Replset: nodeInfo.SetName,
			Node:    nodeInfo.Me,
			OPID:    opid.String(),
			Epoch:   &epts,
		})

		got, err := a.acquireLock(ctx, lck, l)
		if err != nil {
			l.Error("acquiring lock: %v", err)
			return
		}
		if !got {
			l.Debug("skip: lock not acquired")
			l.Error("unable to run the restore while another backup or restore process running")
			return
		}

		defer func() {
			if lck == nil {
				return
			}

			if err := lck.Release(); err != nil {
				l.Error("release lock: %v", err)
			}
		}()

		err = config.SetConfigVar(ctx, a.leadConn, "pitr.enabled", "false")
		if err != nil {
			l.Error("disable oplog slicer: %v", err)
		} else {
			l.Info("oplog slicer disabled")
		}
		a.removePitr()
	}

	stg, err := util.GetStorage(ctx, a.leadConn, l)
	if err != nil {
		l.Error("get storage: %v", err)
		return
	}

	var bcpType defs.BackupType
	bcp := &backup.BackupMeta{}

	if r.External && r.BackupName == "" {
		bcpType = defs.ExternalBackup
	} else {
		l.Info("backup: %s", r.BackupName)
		bcp, err = restore.SnapshotMeta(ctx, a.leadConn, r.BackupName, stg)
		if err != nil {
			l.Error("define base backup: %v", err)
			return
		}

		if !r.OplogTS.IsZero() && bcp.LastWriteTS.Compare(r.OplogTS) >= 0 {
			l.Error("snapshot's last write is later than the target time. " +
				"Try to set an earlier snapshot. Or leave the snapshot empty so PBM will choose one.")
			return
		}
		bcpType = bcp.Type
	}

	l.Info("recovery started")

	switch bcpType {
	case defs.LogicalBackup:
		if !nodeInfo.IsPrimary {
			l.Info("This node is not the primary. Check pbm agent on the primary for restore progress")
			return
		}
		if r.OplogTS.IsZero() {
			err = restore.New(a.leadConn, a.nodeConn, a.brief, r.RSMap).Snapshot(ctx, r, opid, l)
		} else {
			err = restore.New(a.leadConn, a.nodeConn, a.brief, r.RSMap).PITR(ctx, r, opid, l)
		}
	case defs.PhysicalBackup, defs.IncrementalBackup, defs.ExternalBackup:
		if lck != nil {
			// Don't care about errors. Anyway, the lock gonna disappear after the
			// restore. And the commands stream is down as well.
			// The lock also updates its heartbeats but Restore waits only for one state
			// with the timeout twice as short defs.StaleFrameSec.
			_ = lck.Release()
			lck = nil
		}

		var rstr *restore.PhysRestore
		rstr, err = restore.NewPhysical(ctx, a.leadConn, a.nodeConn, nodeInfo, r.RSMap)
		if err != nil {
			l.Error("init physical backup: %v", err)
			return
		}

		r.BackupName = bcp.Name
		err = rstr.Snapshot(ctx, r, r.OplogTS, opid, l, a.closeCMD, a.HbPause)
	}
	if err != nil {
		if errors.Is(err, restore.ErrNoDataForShard) {
			l.Info("no data for the shard in backup, skipping")
		} else {
			l.Error("restore: %v", err)
		}
		return
	}

	if bcpType == defs.LogicalBackup && nodeInfo.IsLeader() {
		epch, err := config.ResetEpoch(ctx, a.leadConn)
		if err != nil {
			l.Error("reset epoch: %v", err)
		}
		l.Debug("epoch set to %v", epch)
	}

	l.Info("recovery successfully finished")
}
