package main

import (
	"context"
	"runtime"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
)

func (a *Agent) Restore(ctx context.Context, r *ctrl.RestoreCmd, opid ctrl.OPID, ep config.Epoch) {
	if r == nil {
		log.Error(ctx, "missed command")
		return
	}

	if !r.OplogTS.IsZero() {
		log.Info(ctx, "to time: %s", time.Unix(int64(r.OplogTS.T), 0).UTC().Format(time.RFC3339))
	}

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		log.Error(ctx, "get node info: %v", err)
		return
	}
	if nodeInfo.ArbiterOnly {
		log.Debug(ctx, "arbiter node. skip")
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

		got, err := a.acquireLock(ctx, lck)
		if err != nil {
			log.Error(ctx, "acquiring lock: %v", err)
			return
		}
		if !got {
			log.Debug(ctx, "skip: lock not acquired")
			log.Error(ctx, "unable to run the restore while another backup or restore process running")
			return
		}

		defer func() {
			if lck == nil {
				return
			}

			if err := lck.Release(); err != nil {
				log.Error(ctx, "release lock: %v", err)
			}
		}()

		err = config.SetConfigVar(ctx, a.leadConn, "pitr.enabled", "false")
		if err != nil {
			log.Error(ctx, "disable oplog slicer: %v", err)
		} else {
			log.Info(ctx, "oplog slicer disabled")
		}
		a.removePitr()
	}

	var bcpType defs.BackupType
	var bcp *backup.BackupMeta

	if r.External && r.BackupName == "" {
		bcpType = defs.ExternalBackup
	} else {
		log.Info(ctx, "backup: %s", r.BackupName)

		// XXX: why is backup searched on storage?
		bcp, err = restore.LookupBackupMeta(ctx, a.leadConn, r.BackupName, defs.NodeID())
		if err != nil {
			err1 := addRestoreMetaWithError(ctx, a.leadConn, opid, r, nodeInfo.SetName,
				"define base backup: %v", err)
			if err1 != nil {
				log.Error(ctx, "failed to save meta: %v", err1)
			}
			return
		}

		if !r.OplogTS.IsZero() && bcp.LastWriteTS.Compare(r.OplogTS) >= 0 {
			err1 := addRestoreMetaWithError(ctx, a.leadConn, opid, r, nodeInfo.SetName,
				"snapshot's last write is later than the target time. "+
					"Try to set an earlier snapshot. Or leave the snapshot empty "+
					"so PBM will choose one.")
			if err1 != nil {
				log.Error(ctx, "failed to save meta: %v", err)
			}
			return
		}
		bcpType = bcp.Type
		r.BackupName = bcp.Name
	}

	cfg, err := config.GetConfig(ctx, a.leadConn)
	if err != nil {
		log.Error(ctx, "get PBM configuration: %v", err)
		return
	}

	log.Info(ctx, "recovery started")

	switch bcpType {
	case defs.LogicalBackup:
		if !nodeInfo.IsPrimary {
			log.Info(ctx, "This node is not the primary. "+
				"Check pbm agent on the primary for restore progress")
			return
		}

		numParallelColls := runtime.NumCPU() / 2
		if r.NumParallelColls != nil && *r.NumParallelColls > 0 {
			numParallelColls = int(*r.NumParallelColls)
		} else if cfg.Restore != nil && cfg.Restore.NumParallelCollections > 0 {
			numParallelColls = cfg.Restore.NumParallelCollections
		}

		rr := restore.New(a.leadConn, a.nodeConn, a.brief, cfg, r.RSMap, numParallelColls)
		if r.OplogTS.IsZero() {
			err = rr.Snapshot(ctx, r, opid, bcp)
		} else {
			err = rr.PITR(ctx, r, opid, bcp)
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
			log.Error(ctx, "init physical backup: %v", err)
			return
		}

		err = rstr.Snapshot(ctx, r, r.OplogTS, opid, a.closeCMD, a.HbPause)
	}
	if err != nil {
		if errors.Is(err, restore.ErrNoDataForShard) {
			log.Info(ctx, "no data for the shard in backup, skipping")
		} else {
			log.Error(ctx, "restore: %v", err)
		}
		return
	}

	if bcpType == defs.LogicalBackup && nodeInfo.IsLeader() {
		epch, err := config.ResetEpoch(ctx, a.leadConn)
		if err != nil {
			log.Error(ctx, "reset epoch: %v", err)
		}
		log.Debug(ctx, "epoch set to %v", epch)
	}

	log.Info(ctx, "recovery successfully finished")
}

func addRestoreMetaWithError(
	ctx context.Context,
	conn connect.Client,
	opid ctrl.OPID,
	cmd *ctrl.RestoreCmd,
	setName string,
	errStr string,
	args ...any,
) error {
	log.Error(ctx, errStr, args...)

	meta := &restore.RestoreMeta{
		Type:     defs.LogicalBackup,
		OPID:     opid.String(),
		Name:     cmd.Name,
		Backup:   cmd.BackupName,
		PITR:     int64(cmd.OplogTS.T),
		StartTS:  time.Now().UTC().Unix(),
		Status:   defs.StatusError,
		Error:    errStr,
		Replsets: []restore.RestoreReplset{},
	}
	err := restore.SetRestoreMetaIfNotExists(ctx, conn, meta)
	if err != nil {
		return errors.Wrap(err, "write restore meta to db")
	}

	rs := restore.RestoreReplset{
		Name:       setName,
		StartTS:    time.Now().UTC().Unix(),
		Status:     defs.StatusError,
		Error:      errStr,
		Conditions: restore.Conditions{},
	}
	err = restore.AddRestoreRSMeta(ctx, conn, cmd.Name, rs)
	if err != nil {
		return errors.Wrap(err, "write backup meta to db")
	}

	return nil
}
