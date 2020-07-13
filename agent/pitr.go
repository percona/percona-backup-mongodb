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

func (a *Agent) PITR() {
	tk := time.NewTicker(pitrCheckPeriod)
	defer tk.Stop()
	for range tk.C {
		err := a.pitr()
		if err != nil {
			a.log.Error(pbm.CmdPITR, "", "%v", err)
		}
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
		if a.pitrjob != nil {
			a.cancelPitr()
		}
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

	// TODO: no need to get it on each cycle
	nodeInfo, err := a.node.GetIsMaster()
	if err != nil {
		return errors.Wrap(err, "get node isMaster data")
	}

	// just a check before a real locking
	// just trying to avoid redundant heavy operations
	ts, err := a.pbm.ClusterTime()
	if err != nil {
		return errors.Wrap(err, "read cluster time")
	}
	tl, err := a.pbm.GetLockData(&pbm.LockHeader{Replset: nodeInfo.SetName}, pbm.LockCollection)
	// ErrNoDocuments or stale lock the only reasons to continue
	if err != mongo.ErrNoDocuments && tl.Heartbeat.T+pbm.StaleFrameSec >= ts.T {
		return errors.Wrap(err, "check if already run")
	}

	ibcp, err := pitr.NewBackup(nodeInfo.SetName, a.pbm, a.node)
	if err != nil {
		return errors.Wrap(err, "create backup object")
	}

	err = ibcp.Catchup()
	if err != nil {
		return errors.Wrap(err, "defining starting point for the backup")
	}

	stg, err := a.pbm.GetStorage()
	if err != nil {
		return errors.Wrap(err, "unable to get storage configuration")
	}

	lock := a.pbm.NewLock(pbm.LockHeader{
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
		Type:    pbm.CmdPITR,
	})

	got, err := a.aquireLock(lock, nil)
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
				a.log.Error(pbm.CmdPITR, "", "release lock: %v", err)
			}
		}()

		ctx, cancel := context.WithCancel(context.Background())
		w := make(chan struct{})

		a.setPitr(&currentPitr{
			cancel: cancel,
			wakeup: w,
		})

		err := ibcp.Stream(ctx, w, stg, pbm.CompressionTypeS2)
		if err != nil {
			a.log.Error(pbm.CmdPITR, "", "streaming oplog: %v", err)
		}

		a.unsetPitr()
	}()

	return nil
}

// PITRestore starts the point-in-time recovery
func (a *Agent) PITRestore(r pbm.PITRestoreCmd) {
	tsstr := time.Unix(int64(r.TS), 0).UTC().Format(time.RFC3339)
	nodeInfo, err := a.node.GetIsMaster()
	if err != nil {
		a.log.Error(pbm.CmdPITR, tsstr, "get node info: %v", err)
		return
	}
	if !nodeInfo.IsMaster {
		a.log.Info(pbm.CmdPITR, tsstr, "Node in not suitable for restore")
		return
	}

	lock := a.pbm.NewLock(pbm.LockHeader{
		Type:    pbm.CmdPITRestore,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
	})

	got, err := lock.Acquire()
	if err != nil {
		a.log.Error(pbm.CmdPITR, tsstr, "acquiring lock: %v", err)
		return
	}
	if !got {
		a.log.Error(pbm.CmdPITR, tsstr, "unbale to run the restore while another backup or restore process running")
		return
	}

	defer func() {
		err := lock.Release()
		if err != nil {
			a.log.Error(pbm.CmdPITR, tsstr, "release lock: %v", err)
		}
	}()

	a.log.Info(pbm.CmdPITR, tsstr, "recovery started")
	err = restore.New(a.pbm, a.node).PITR(r)
	if err != nil {
		a.log.Error(pbm.CmdPITR, tsstr, "restore: %v", err)
		return
	}
	a.log.Info(pbm.CmdPITR, tsstr, "recovery successfully finished")
}
