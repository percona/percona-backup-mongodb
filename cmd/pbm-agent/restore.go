package main

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
	"github.com/percona/percona-backup-mongodb/pbm/slicer"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/util"
)

type currentPitr struct {
	slicer *slicer.Slicer
	w      chan ctrl.OPID // to wake up a slicer on demand (not to wait for the tick)
	cancel context.CancelFunc
}

func (a *Agent) setPitr(p *currentPitr) {
	a.mx.Lock()
	defer a.mx.Unlock()

	if a.pitrjob != nil {
		a.pitrjob.cancel()
	}

	a.pitrjob = p
}

func (a *Agent) removePitr() {
	a.setPitr(nil)
}

func (a *Agent) getPitr() *currentPitr {
	a.mx.Lock()
	defer a.mx.Unlock()

	return a.pitrjob
}

func (a *Agent) sliceNow(opid ctrl.OPID) {
	a.mx.Lock()
	defer a.mx.Unlock()

	if a.pitrjob == nil {
		return
	}

	a.pitrjob.w <- opid
}

const pitrCheckPeriod = time.Second * 15

// PITR starts PITR processing routine
func (a *Agent) PITR(ctx context.Context) {
	l := log.FromContext(ctx)
	l.Printf("starting PITR routine")

	for {
		wait := pitrCheckPeriod

		err := a.pitr(ctx)
		if err != nil {
			// we need epoch just to log pitr err with an extra context
			// so not much care if we get it or not
			ep, _ := config.GetEpoch(ctx, a.leadConn)
			l.Error(string(ctrl.CmdPITR), "", "", ep.TS(), "init: %v", err)

			// penalty to the failed node so healthy nodes would have priority on next try
			wait *= 2
		}

		time.Sleep(wait)
	}
}

func (a *Agent) stopPitrOnOplogOnlyChange(currOO bool) {
	if a.prevOO == nil {
		a.prevOO = &currOO
		return
	}

	if *a.prevOO == currOO {
		return
	}

	a.prevOO = &currOO
	a.removePitr()
}

// canSlicingNow returns lock.ConcurrentOpError if there is a parallel operation.
// Only physical backups (full, incremental, external) is allowed.
func canSlicingNow(ctx context.Context, conn connect.Client) error {
	locks, err := lock.GetLocks(ctx, conn, &lock.LockHeader{})
	if err != nil {
		return errors.Wrap(err, "get locks data")
	}

	for i := range locks {
		l := &locks[i]

		if l.Type != ctrl.CmdBackup {
			return lock.ConcurrentOpError{l.LockHeader}
		}

		bcp, err := backup.GetBackupByOPID(ctx, conn, l.OPID)
		if err != nil {
			return errors.Wrap(err, "get backup metadata")
		}

		if bcp.Type == defs.LogicalBackup {
			return lock.ConcurrentOpError{l.LockHeader}
		}
	}

	return nil
}

func (a *Agent) pitr(ctx context.Context) error {
	cfg, err := config.GetConfig(ctx, a.leadConn)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return errors.Wrap(err, "get conf")
		}
		cfg = &config.Config{}
	}

	a.stopPitrOnOplogOnlyChange(cfg.PITR.OplogOnly)

	if !cfg.PITR.Enabled {
		a.removePitr()
		return nil
	}

	ep := config.Epoch(cfg.Epoch)
	l := log.FromContext(ctx).NewEvent(string(ctrl.CmdPITR), "", "", ep.TS())
	ctx = log.SetLogEventToContext(ctx, l)

	if err := canSlicingNow(ctx, a.leadConn); err != nil {
		e := lock.ConcurrentOpError{}
		if errors.As(err, &e) {
			l.Info("oplog slicer is paused for lock [%s, opid: %s]", e.Lock.Type, e.Lock.OPID)
			return nil
		}

		return err
	}

	slicerInterval := cfg.OplogSlicerInterval()

	if p := a.getPitr(); p != nil {
		// already do the job
		currInterval := p.slicer.GetSpan()
		if currInterval != slicerInterval {
			p.slicer.SetSpan(slicerInterval)

			// wake up slicer only if a new interval is smaller
			if currInterval > slicerInterval {
				a.sliceNow(ctrl.NilOPID)
			}
		}

		return nil
	}

	// just a check before a real locking
	// just trying to avoid redundant heavy operations
	moveOn, err := a.pitrLockCheck(ctx)
	if err != nil {
		return errors.Wrap(err, "check if already run")
	}

	if !moveOn {
		return nil
	}

	// should be after the lock pre-check
	//
	// if node failing, then some other agent with healthy node will hopefully catch up
	// so this code won't be reached and will not pollute log with "pitr" errors while
	// the other node does successfully slice
	ninf, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		return errors.Wrap(err, "get node info")
	}
	q, err := topo.NodeSuits(ctx, a.nodeConn, ninf)
	if err != nil {
		return errors.Wrap(err, "node check")
	}

	// node is not suitable for doing backup
	if !q {
		return nil
	}

	epts := ep.TS()
	lck := lock.NewOpLock(a.leadConn, lock.LockHeader{
		Replset: a.brief.SetName,
		Node:    a.brief.Me,
		Type:    ctrl.CmdPITR,
		Epoch:   &epts,
	})

	got, err := a.acquireLock(ctx, lck, l)
	if err != nil {
		return errors.Wrap(err, "acquiring lock")
	}
	if !got {
		l.Debug("skip: lock not acquired")
		return nil
	}

	stg, err := util.StorageFromConfig(cfg.Storage, l)
	if err != nil {
		return errors.Wrap(err, "unable to get storage configuration")
	}

	ibcp := slicer.NewSlicer(a.brief.SetName, a.leadConn, a.nodeConn, stg, cfg, log.FromContext(ctx))
	ibcp.SetSpan(slicerInterval)

	if cfg.PITR.OplogOnly {
		err = ibcp.OplogOnlyCatchup(ctx)
	} else {
		err = ibcp.Catchup(ctx)
	}
	if err != nil {
		if err := lck.Release(); err != nil {
			l.Error("release lock: %v", err)
		}
		return errors.Wrap(err, "catchup")
	}

	go func() {
		stopSlicingCtx, stopSlicing := context.WithCancel(ctx)
		defer stopSlicing()
		stopC := make(chan struct{})

		w := make(chan ctrl.OPID)
		a.setPitr(&currentPitr{
			slicer: ibcp,
			cancel: stopSlicing,
			w:      w,
		})

		go func() {
			<-stopSlicingCtx.Done()
			close(stopC)
			a.removePitr()
		}()

		streamErr := ibcp.Stream(ctx,
			stopC,
			w,
			cfg.PITR.Compression,
			cfg.PITR.CompressionLevel,
			cfg.Backup.Timeouts)
		if streamErr != nil {
			out := l.Error
			if errors.Is(streamErr, slicer.OpMovedError{}) {
				out = l.Info
			}
			out("streaming oplog: %v", streamErr)
		}

		if err := lck.Release(); err != nil {
			l.Error("release lock: %v", err)
		}

		// Penalty to the failed node so healthy nodes would have priority on next try.
		// But lock has to be released first. Otherwise, healthy nodes would wait for the lock release
		// and the penalty won't have any sense.
		if streamErr != nil {
			time.Sleep(pitrCheckPeriod * 2)
		}
	}()

	return nil
}

func (a *Agent) pitrLockCheck(ctx context.Context) (bool, error) {
	ts, err := topo.GetClusterTime(ctx, a.leadConn)
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	tl, err := lock.GetOpLockData(ctx, a.leadConn, &lock.LockHeader{
		Replset: a.brief.SetName,
		Type:    ctrl.CmdPITR,
	})
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			// no lock. good to move on
			return true, nil
		}

		return false, errors.Wrap(err, "get lock")
	}

	// stale lock means we should move on and clean it up during the lock.Acquire
	return tl.Heartbeat.T+defs.StaleFrameSec < ts.T, nil
}

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
			l.Info("Node in not suitable for restore")
			return
		}
		if r.OplogTS.IsZero() {
			err = restore.New(a.leadConn, a.nodeConn, a.brief, r.RSMap).Snapshot(ctx, r, opid, l)
		} else {
			err = restore.New(a.leadConn, a.nodeConn, a.brief, r.RSMap).PITR(ctx, r, opid, l)
		}
	case defs.PhysicalBackup, defs.IncrementalBackup, defs.ExternalBackup:
		if bcpType == defs.PhysicalBackup && util.IsSelective(r.Namespaces) {
			err = a.doSelectivePhysicalRestore(ctx, r, opid)
		} else {
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
				l.Error("init physical restore: %v", err)
				return
			}

			r.BackupName = bcp.Name
			err = rstr.Snapshot(ctx, r, r.OplogTS, opid, l, a.closeCMD, a.HbPause)
		}
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

//nolint:nonamedreturns
func (a *Agent) doSelectivePhysicalRestore(ctx context.Context, r *ctrl.RestoreCmd, opid ctrl.OPID) (err error) {
	nodeInfo, err := topo.GetNodeInfo(ctx, a.nodeConn)
	if err != nil {
		return errors.Wrap(err, "get node info")
	}
	bcpMeta, err := backup.NewDBManager(a.leadConn).GetBackupByName(ctx, r.BackupName)
	if err != nil {
		return errors.Wrap(err, "get backup meta")
	}
	config, err := config.GetConfig(ctx, a.leadConn)
	if err != nil {
		return errors.Wrap(err, "get config")
	}
	storage, err := util.StorageFromConfig(config.Storage, log.LogEventFromContext(ctx))
	if err != nil {
		return errors.Wrap(err, "storage from config")
	}

	restoreOptions := restore.SelectivePhysicalOptions{
		LeaderConn: a.leadConn,
		NodeConn:   a.nodeConn,
		NodeInfo:   nodeInfo,
		Config:     config,
		Storage:    storage,
		Name:       r.Name,
		Backup:     bcpMeta,
		Namespaces: r.Namespaces,
	}
	res, err := restore.NewSelectivePhysical(ctx, restoreOptions)
	if err != nil {
		return errors.Wrap(err, "new restore")
	}
	defer func() {
		if v := recover(); v != nil {
			err = errors.Errorf("panicked with: %v", v)
		}
		if err == nil {
			return
		}

		closeError := res.CloseWithError(context.Background(), err)
		if closeError != nil {
			l := log.LogEventFromContext(ctx)
			l.Error("close restore: %v", closeError)
		}
	}()

	err = res.Init(ctx, opid)
	if err != nil {
		return errors.Wrap(err, "init")
	}

	err = res.Run(ctx)
	if err != nil {
		return errors.Wrap(err, "run")
	}

	err = res.Done(ctx)
	if err != nil {
		return errors.Wrap(err, "done")
	}

	return nil
}
