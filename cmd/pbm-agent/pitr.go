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
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/prio"
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
const pitrRenominationFrame = 5 * time.Second

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

	slicerInterval := cfg.OplogSlicerInterval()

	ep := config.Epoch(cfg.Epoch)
	l := log.FromContext(ctx).NewEvent(string(ctrl.CmdPITR), "", "", ep.TS())
	ctx = log.SetLogEventToContext(ctx, l)

	if !cfg.PITR.Enabled {
		a.removePitr()
		return nil
	}

	a.stopPitrOnOplogOnlyChange(cfg.PITR.OplogOnly)

	if err := canSlicingNow(ctx, a.leadConn); err != nil {
		e := lock.ConcurrentOpError{}
		if errors.As(err, &e) {
			l.Info("oplog slicer is paused for lock [%s, opid: %s]", e.Lock.Type, e.Lock.OPID)
			return nil
		}

		return err
	}

	if p := a.getPitr(); p != nil {
		// already do the job
		//todo: remove this span changing detaction to leader
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
		l.Debug("pitr running on another RS member")
		return nil
	}

	// should be after the lock pre-check
	//
	// if node failing, then some other agent with healthy node will hopefully catch up
	// so this code won't be reached and will not pollute log with "pitr" errors while
	// the other node does successfully slice
	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		l.Error("get node info: %v", err)
		return errors.Wrap(err, "get node info")
	}

	q, err := topo.NodeSuits(ctx, a.nodeConn, nodeInfo)
	if err != nil {
		return errors.Wrap(err, "node check")
	}
	// node is not suitable for doing pitr
	if !q {
		return nil
	}

	isClusterLeader := nodeInfo.IsClusterLeader()

	if isClusterLeader {
		//todo: init meta on first usage
		//oplog.InitMeta(ctx, a.leadConn)

		agents, err := topo.ListAgentStatuses(ctx, a.leadConn)
		if err != nil {
			l.Error("get agents list: %v", err)
			return errors.Wrap(err, "list agents statuses")
		}

		nodes, err := prio.CalcNodesPriority(ctx, nil, cfg.Backup.Priority, agents)
		if err != nil {
			l.Error("get nodes priority: %v", err)
			return errors.Wrap(err, "get nodes priorities")
		}

		shards, err := topo.ClusterMembers(ctx, a.leadConn.MongoClient())
		if err != nil {
			l.Error("get cluster members: %v", err)
			return errors.Wrap(err, "get cluster members")
		}

		for _, sh := range shards {
			go func(rs string) {
				if err := a.nominateRSForPITR(ctx, rs, nodes.RS(rs)); err != nil {
					l.Error("nodes nomination error for %s: %v", rs, err)
				}
			}(sh.RS)
		}
	}

	nominated, err := a.waitNominationForPITR(ctx, nodeInfo.SetName, nodeInfo.Me)
	if err != nil {
		l.Error("wait for pitr nomination: %v", err)
		return errors.Wrap(err, "wait nomination for pitr")
	}
	if !nominated {
		l.Debug("skip after pitr nomination, probably started by another node")
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
	err = oplog.SetPITRNomineeACK(ctx, a.leadConn, a.brief.SetName, a.brief.Me)

	stg, err := util.StorageFromConfig(cfg.Storage, l)
	if err != nil {
		return errors.Wrap(err, "unable to get storage configuration")
	}

	s := slicer.NewSlicer(a.brief.SetName, a.leadConn, a.nodeConn, stg, cfg, log.FromContext(ctx))
	s.SetSpan(slicerInterval)

	if cfg.PITR.OplogOnly {
		err = s.OplogOnlyCatchup(ctx)
	} else {
		err = s.Catchup(ctx)
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
			slicer: s,
			cancel: stopSlicing,
			w:      w,
		})

		go func() {
			<-stopSlicingCtx.Done()
			close(stopC)
			a.removePitr()
		}()

		streamErr := s.Stream(ctx,
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

func (a *Agent) nominateRSForPITR(ctx context.Context, rs string, nodes [][]string) error {
	l := log.LogEventFromContext(ctx)
	l.Debug("pitr nomination list for %s: %v", rs, nodes)
	err := oplog.SetPITRNomination(ctx, a.leadConn, rs)
	if err != nil {
		return errors.Wrap(err, "set pitr nomination meta")
	}

	for _, n := range nodes {
		nms, err := oplog.GetPITRNominees(ctx, a.leadConn, rs)
		if err != nil && !errors.Is(err, errors.ErrNotFound) {
			return errors.Wrap(err, "get pitr nominees")
		}
		if nms != nil && len(nms.Ack) > 0 {
			l.Debug("pitr nomination: %s won by %s", rs, nms.Ack)
			return nil
		}

		err = oplog.SetPITRNominees(ctx, a.leadConn, rs, n)
		if err != nil {
			return errors.Wrap(err, "set pitr nominees")
		}
		l.Debug("pitr nomination %s, set candidates %v", rs, n)

		time.Sleep(pitrRenominationFrame)
	}

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

func (a *Agent) waitNominationForPITR(ctx context.Context, rs, node string) (bool, error) {
	l := log.LogEventFromContext(ctx)

	tk := time.NewTicker(time.Millisecond * 500)
	defer tk.Stop()

	l.Debug("waiting pitr nomination")
	for {
		select {
		case <-tk.C:

			nm, err := oplog.GetPITRNominees(ctx, a.leadConn, rs)
			if err != nil {
				if errors.Is(err, errors.ErrNotFound) {
					continue
				}
				return false, errors.Wrap(err, "check pitr nomination")
			}
			if len(nm.Ack) > 0 {
				return false, nil
			}
			for _, n := range nm.Nodes {
				if n == node {
					return true, nil
				}
			}
		}
		//todo: we should handle cancelation here also, for e.g.:
		// - pitr is disabled
		// - configuration has been changed
		// - cluster topology has been changed
	}
}
