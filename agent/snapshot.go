package agent

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
)

type currentBackup struct {
	header *pbm.BackupCmd
	cancel context.CancelFunc
}

func (a *Agent) setBcp(b *currentBackup) (changed bool) {
	a.mx.Lock()
	defer a.mx.Unlock()
	if a.bcp != nil {
		return false
	}

	a.bcp = b
	return true
}

func (a *Agent) unsetBcp() {
	a.mx.Lock()
	a.bcp = nil
	a.mx.Unlock()
}

// CancelBackup cancels current backup
func (a *Agent) CancelBackup() {
	a.mx.Lock()
	defer a.mx.Unlock()
	if a.bcp == nil {
		return
	}

	a.bcp.cancel()
}

// Backup starts backup
func (a *Agent) Backup(bcp pbm.BackupCmd) {
	nodeInfo, err := a.node.GetInfo()
	if err != nil {
		a.log.Error(pbm.CmdBackup, bcp.Name, "get node info: %v", err)
		return
	}

	// In case there are some leftovers from the restore.
	//
	// There is no way to exclude some collections on mongodump stage
	// while it accepts "exclude..." option only if sole db specified (¯\_(ツ)_/¯)
	// So we're trying to clean up on primary node but because there is no guarantee
	// that primary node reaches this code before some of the secondaries starts backup
	// we have to double-check in backup.run() before mongodump and fail on dirty state.
	if nodeInfo.IsPrimary && nodeInfo.Me == nodeInfo.Primary {
		err = a.node.DropTMPcoll()
		if err != nil {
			a.log.Error(pbm.CmdBackup, bcp.Name, "ensure no tmp collections: %v", err)
			return
		}
	}

	q, err := backup.NodeSuits(a.node, nodeInfo)
	if err != nil {
		a.log.Error(pbm.CmdBackup, bcp.Name, "node check: %v", err)
		return
	}

	// node is not suitable for doing backup
	if !q {
		a.log.Info(pbm.CmdBackup, bcp.Name, "node in not suitable for backup")
		return
	}

	// workaround for pitr
	//
	// 1. set a backup's intent so the PITR routine won't try to proceed, hence acquire a lock
	// 2. wait for pitrCheckPeriod * 1.1 to be sure PITR routine observed the state
	// 3. remove PITR lock and wake up PITR backup process so it may finish its PITR-stuff
	//    (nothing gonna be done if no PITR process run by current agent)
	// 4. try to acquire a backup's lock and move further with the backup
	// 5. despite any further plot development - clear the backup's intent to unblock PITR routine before return
	atomic.StoreUint32(&a.intent, intentBackup)
	defer atomic.StoreUint32(&a.intent, intentNone)

	time.Sleep(pitrCheckPeriod * 11 / 10)

	err = a.pbm.NewLock(pbm.LockHeader{
		Type:    pbm.CmdPITR,
		Replset: nodeInfo.SetName,
	}).Release()
	if err != nil {
		a.log.Warning(pbm.CmdBackup, bcp.Name, "clearing pitr locks: %v", err)
	}
	// wakeup the slicer not to wait for the tick
	a.wakeupPitr()

	lock := a.pbm.NewLock(pbm.LockHeader{
		Type:       pbm.CmdBackup,
		Replset:    nodeInfo.SetName,
		Node:       nodeInfo.Me,
		BackupName: bcp.Name,
	})

	got, err := a.aquireLock(lock, a.pbm.MarkBcpStale)
	if err != nil {
		a.log.Error(pbm.CmdBackup, bcp.Name, "acquiring lock: %v", err)
		return
	}
	if !got {
		a.log.Info(pbm.CmdBackup, bcp.Name, "backup has been scheduled on another replset node")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	a.setBcp(&currentBackup{
		header: &bcp,
		cancel: cancel,
	})
	a.log.Info(pbm.CmdBackup, bcp.Name, "backup started")
	tstart := time.Now()
	err = backup.New(ctx, a.pbm, a.node).Run(bcp)
	a.unsetBcp()
	if err != nil {
		if errors.Is(err, backup.ErrCancelled) {
			a.log.Info(pbm.CmdBackup, bcp.Name, "backup was canceled")
		} else {
			a.log.Error(pbm.CmdBackup, bcp.Name, "backup: %v", err)
		}
	} else {
		a.log.Info(pbm.CmdBackup, bcp.Name, "backup finished")
	}

	// Update PITR "changed" option to "reset" observation by pbm list of
	// any PITR related errors in the log since some of the errors might
	// be fixed by the backup. If not (errors wasn't fixed by bcp) PITR will
	// generate new errors so we won't miss anything
	err = a.pbm.ConfigBumpPITRepoch()
	if err != nil {
		a.log.Warning(pbm.CmdBackup, bcp.Name, "update PITR obesrvation ts")
	}

	// In the case of fast backup (small db) we have to wait before releasing the lock.
	// Otherwise, since the primary node waits for `WaitBackupStart*0.9` before trying to acquire the lock
	// it might happen that the backup will be made twice:
	//
	// secondary1 >---------*!lock(fail - acuired by s1)---------------------------
	// secondary2 >------*lock====backup====*unlock--------------------------------
	// primary    >--------*wait--------------------*lock====backup====*unlock-----
	//
	// Secondaries also may start trying to acquire a lock with quite an interval (e.g. due to network issues)
	// TODO: we cannot rely on the nodes wall clock.
	// TODO: ? pbmBackups should have unique index by name ?
	needToWait := pbm.WaitBackupStart - time.Since(tstart)
	if needToWait > 0 {
		time.Sleep(needToWait)
	}
	err = lock.Release()
	if err != nil {
		a.log.Error(pbm.CmdBackup, bcp.Name, "unable to release backup lock %v: %v", lock, err)
	}
}

// Restore starts the restore
func (a *Agent) Restore(r pbm.RestoreCmd) {
	nodeInfo, err := a.node.GetInfo()
	if err != nil {
		a.log.Error(pbm.CmdRestore, r.BackupName, "get node info: %v", err)
		return
	}
	if !nodeInfo.IsPrimary {
		a.log.Info(pbm.CmdRestore, r.BackupName, "node in not suitable for restore")
		return
	}

	lock := a.pbm.NewLock(pbm.LockHeader{
		Type:       pbm.CmdRestore,
		Replset:    nodeInfo.SetName,
		Node:       nodeInfo.Me,
		BackupName: r.Name,
	})

	got, err := lock.Acquire()
	if err != nil {
		a.log.Error(pbm.CmdRestore, r.BackupName, "acquiring lock: %v", err)
		return
	}
	if !got {
		a.log.Error(pbm.CmdRestore, r.BackupName, "unbale to run the restore while another backup or restore process running")
		return
	}

	defer func() {
		err := lock.Release()
		if err != nil {
			a.log.Error(pbm.CmdRestore, r.BackupName, "release lock: %v", err)
		}
	}()

	a.log.Info(pbm.CmdRestore, r.BackupName, "restore started")
	err = restore.New(a.pbm, a.node).Snapshot(r)
	if err != nil {
		a.log.Error(pbm.CmdRestore, r.BackupName, "restore: %v", err)
		return
	}
	a.log.Info(pbm.CmdRestore, r.BackupName, "restore finished successfully")
}
