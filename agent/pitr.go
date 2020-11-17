package agent

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/pitr"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
)

type currentPitr struct {
	wakeup chan struct{} // to wake up a slicer on demand (not to wait for the tick)
	cancel context.CancelFunc
}

func (a *Agent) setPitr(p *currentPitr) (changed bool) {
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

func (a *Agent) ispitr() bool {
	a.mx.Lock()
	defer a.mx.Unlock()
	return a.pitrjob != nil
}

func (a *Agent) cancelPitr() {
	a.mx.Lock()
	defer a.mx.Unlock()
	if a.pitrjob == nil {
		return
	}

	a.pitrjob.cancel()
}

func (a *Agent) wakeupPitr() {
	a.mx.Lock()
	defer a.mx.Unlock()
	if a.pitrjob == nil {
		return
	}

	a.pitrjob.wakeup <- struct{}{}
}

const pitrCheckPeriod = time.Second * 15

// PITR starts PITR prcessing routine
func (a *Agent) PITR() {
	a.log.Printf("starting PITR routine")

	for {
		err := a.pitr()
		if err != nil {
			ep, _ := a.pbm.GetEpoch()
			a.log.Error(string(pbm.CmdPITR), "", "", ep.TS(), "%v", err)
		}

		time.Sleep(pitrCheckPeriod)
	}
}

func (a *Agent) pitr() (err error) {
	if atomic.LoadUint32(&a.intent) == intentBackup {
		return nil
	}

	on, err := a.pbm.IsPITR()
	if err != nil {
		return errors.Wrap(err, "check if on")
	}
	if !on {
		a.cancelPitr()
		return nil
	}

	// we already do the job
	if on && a.ispitr() {
		return nil
	}

	q, err := backup.NodeSuits(a.node)
	if err != nil {
		return errors.Wrap(err, "node check")
	}

	// node is not suitable for doing backup
	if !q {
		return nil
	}

	// just a check before a real locking
	// just trying to avoid redundant heavy operations
	ts, err := a.pbm.ClusterTime()
	if err != nil {
		return errors.Wrap(err, "read cluster time")
	}
	tl, err := a.pbm.GetLockData(&pbm.LockHeader{Replset: a.node.RS()})
	// ErrNoDocuments or stale lock the only reasons to continue
	if err != mongo.ErrNoDocuments && tl.Heartbeat.T+pbm.StaleFrameSec >= ts.T {
		return errors.Wrap(err, "check if already run")
	}

	ibcp := pitr.NewBackup(a.node.RS(), a.pbm, a.node)

	err = ibcp.Catchup()
	if err != nil {
		return errors.Wrap(err, "defining starting point for the backup")
	}

	ep, err := a.pbm.GetEpoch()
	if err != nil {
		return errors.Wrap(err, "get epoch")
	}
	l := a.log.NewEvent(string(pbm.CmdPITR), "", "", ep.TS())

	stg, err := a.pbm.GetStorage(l)
	if err != nil {
		return errors.Wrap(err, "unable to get storage configuration")
	}

	lock := a.pbm.NewLock(pbm.LockHeader{
		Replset: a.node.RS(),
		Node:    a.node.Name(),
		Type:    pbm.CmdPITR,
		Epoch:   ep.TS(),
	})

	got, err := a.aquireLock(lock)
	if err != nil {
		return errors.Wrap(err, "acquiring lock")
	}
	if !got {
		return nil
	}

	go func() {
		defer func() {
			err := lock.Release()
			if err != nil {
				l.Error("release lock: %v", err)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		w := make(chan struct{})

		a.setPitr(&currentPitr{
			cancel: cancel,
			wakeup: w,
		})

		err := ibcp.Stream(ctx, ep, w, stg, pbm.CompressionTypeS2)
		if err != nil {
			switch err.(type) {
			case pitr.ErrOpMoved:
				l.Info("streaming oplog: %v", err)
			default:
				l.Error("streaming oplog: %v", err)
			}
		}

		a.unsetPitr()
	}()

	return nil
}

// PITRestore starts the point-in-time recovery
func (a *Agent) PITRestore(r pbm.PITRestoreCmd, opid pbm.OPID, ep pbm.Epoch) {
	l := a.log.NewEvent(string(pbm.CmdPITR), time.Unix(r.TS, 0).UTC().Format(time.RFC3339), opid.String(), ep.TS())

	nodeInfo, err := a.node.GetInfo()
	if err != nil {
		l.Error("get node info: %v", err)
		return
	}
	if !nodeInfo.IsPrimary {
		l.Info("Node in not suitable for restore")
		return
	}

	lock := a.pbm.NewLock(pbm.LockHeader{
		Type:    pbm.CmdPITRestore,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
		OPID:    opid.String(),
		Epoch:   ep.TS(),
	})

	got, err := a.aquireLock(lock)
	if err != nil {
		l.Error("acquiring lock: %v", err)
		return
	}
	if !got {
		l.Error("unbale to run the restore while another backup or restore process running")
		return
	}

	defer func() {
		err := lock.Release()
		if err != nil {
			l.Error("release lock: %v", err)
		}
	}()

	l.Info("recovery started")
	err = restore.New(a.pbm, a.node).PITR(r, opid, l)
	if err != nil {
		l.Error("restore: %v", err)
		return
	}
	l.Info("recovery successfully finished")
}
