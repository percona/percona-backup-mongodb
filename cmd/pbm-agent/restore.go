package main

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/internal/backup"
	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/ctrl"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/lock"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/restore"
	"github.com/percona/percona-backup-mongodb/internal/slicer"
	"github.com/percona/percona-backup-mongodb/internal/topo"
	"github.com/percona/percona-backup-mongodb/internal/util"
)

type currentPitr struct {
	slicer *slicer.Slicer
	w      chan *ctrl.OPID // to wake up a slicer on demand (not to wait for the tick)
	cancel context.CancelFunc
}

func (a *Agent) setPitr(p *currentPitr) bool {
	a.mx.Lock()
	defer a.mx.Unlock()
	if a.pitrjob != nil {
		return false
	}

	a.pitrjob = p
	return true
}

func (a *Agent) unsetPitr() {
	a.mx.Lock()
	a.pitrjob = nil
	a.mx.Unlock()
}

func (a *Agent) getPitr() *currentPitr {
	a.mx.Lock()
	defer a.mx.Unlock()
	return a.pitrjob
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

	if p := a.getPitr(); p != nil {
		p.cancel()
		a.unsetPitr()
	}
}

func (a *Agent) pitr(ctx context.Context) error {
	// pausing for physical restore
	if !a.HbIsRun() {
		return nil
	}

	cfg, err := config.GetConfig(ctx, a.leadConn)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return errors.Wrap(err, "get conf")
		}
		cfg = &config.Config{}
	}

	a.stopPitrOnOplogOnlyChange(cfg.PITR.OplogOnly)
	p := a.getPitr()

	if !cfg.PITR.Enabled {
		if p != nil {
			p.cancel()
		}
		return nil
	}

	ep, err := config.GetEpoch(ctx, a.leadConn)
	if err != nil {
		return errors.Wrap(err, "get epoch")
	}

	l := log.FromContext(ctx).NewEvent(string(ctrl.CmdPITR), "", "", ep.TS())

	spant := time.Duration(cfg.PITR.OplogSpanMin * float64(time.Minute))
	if spant == 0 {
		spant = defs.PITRdefaultSpan
	}

	// already do the job
	if p != nil {
		// update slicer span
		cspan := p.slicer.GetSpan()
		if p.slicer != nil && cspan != spant {
			l.Debug("set pitr span to %v", spant)
			p.slicer.SetSpan(spant)

			// wake up slicer only if span became smaller
			if spant < cspan {
				a.pitrjob.w <- nil
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

	stg, err := util.GetStorage(ctx, a.leadConn, l)
	if err != nil {
		return errors.Wrap(err, "unable to get storage configuration")
	}

	epts := ep.TS()
	lck := lock.NewLock(a.leadConn, lock.LockHeader{
		Replset: a.brief.SetName,
		Node:    a.brief.Me,
		Type:    ctrl.CmdPITR,
		Epoch:   &epts,
	})

	got, err := a.acquireLock(ctx, lck, l, nil)
	if err != nil {
		return errors.Wrap(err, "acquiring lock")
	}
	if !got {
		l.Debug("skip: lock not acquired")
		return nil
	}

	ibcp := slicer.NewSlicer(a.brief.SetName, a.leadConn, a.nodeConn, stg, ep,
		log.FromContext(ctx))
	ibcp.SetSpan(spant)

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

		w := make(chan *ctrl.OPID, 1)
		a.setPitr(&currentPitr{
			slicer: ibcp,
			cancel: stopSlicing,
			w:      w,
		})

		go func() {
			<-stopSlicingCtx.Done()
			close(stopC)
		}()

		streamErr := ibcp.Stream(ctx, stopC, w, cfg.PITR.Compression, cfg.PITR.CompressionLevel, cfg.Backup.Timeouts)
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

		a.unsetPitr()
	}()

	return nil
}

func (a *Agent) pitrLockCheck(ctx context.Context) (bool, error) {
	ts, err := topo.GetClusterTime(ctx, a.leadConn)
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	tl, err := lock.GetLockData(ctx, a.leadConn, &lock.LockHeader{Replset: a.brief.SetName})
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

		got, err := a.acquireLock(ctx, lck, l, nil)
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
		epch, err := config.ResetEpoch(a.leadConn)
		if err != nil {
			l.Error("reset epoch: %v", err)
		}
		l.Debug("epoch set to %v", epch)
	}

	l.Info("recovery successfully finished")
}
