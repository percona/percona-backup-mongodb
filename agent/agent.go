package agent

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
)

type Agent struct {
	pbm     *pbm.PBM
	node    *pbm.Node
	bcp     *currentBackup
	pitrjob *currentPitr
	mx      sync.Mutex
	intent  uint32
}

const (
	intentNone uint32 = iota
	intentBackup
)

type currentBackup struct {
	header *pbm.BackupCmd
	cancel context.CancelFunc
}

type currentPitr struct {
	wakeup chan struct{} // to wake up a slicer on demand (not to wait for the tick)
	cancel context.CancelFunc
}

func New(pbm *pbm.PBM) *Agent {
	return &Agent{
		pbm: pbm,
	}
}

func (a *Agent) AddNode(ctx context.Context, curi string) (err error) {
	a.node, err = pbm.NewNode(ctx, "node0", curi)
	return err
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

// Start starts listening the commands stream.
func (a *Agent) Start() error {
	nodeInfo, err := a.node.GetIsMaster()
	if err != nil {
		log.Println("[Warning] get node isMaster data:", err)
	}
	log.Printf("node: %s/%s", nodeInfo.SetName, nodeInfo.Me)

	c, cerr, err := a.pbm.ListenCmd()
	if err != nil {
		return err
	}

	for {
		select {
		case cmd := <-c:
			log.Printf("Got command %s [%v]", cmd.Cmd, cmd)
			switch cmd.Cmd {
			case pbm.CmdBackup:
				go a.Backup(cmd.Backup)
			case pbm.CmdCancelBackup:
				a.CancelBackup()
			case pbm.CmdRestore:
				a.Restore(cmd.Restore)
			case pbm.CmdResyncBackupList:
				a.ResyncBackupList()
			}
		case err := <-cerr:
			switch err.(type) {
			case pbm.ErrorCursor:
				return errors.Wrap(err, "stop listening")
			default:
				// channel closed / cursor is empty
				if err == nil {
					return errors.New("change stream was closed")
				}

				log.Println("[ERROR] listening commands:", err)
			}
		}
	}
}

// Backup starts backup
func (a *Agent) Backup(bcp pbm.BackupCmd) {
	q, err := backup.NodeSuits(a.node)
	if err != nil {
		log.Println("[ERROR] backup: node check:", err)
		return
	}

	// node is not suitable for doing backup
	if !q {
		log.Println("Node in not suitable for backup")
		return
	}

	nodeInfo, err := a.node.GetIsMaster()
	if err != nil {
		log.Println("[ERROR] backup: get node isMaster data:", err)
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
		log.Println("[Warning] backup: clearing pitr locks:", err)
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
		log.Println("[ERROR] backup: acquiring lock:", err)
		return
	}
	if !got {
		log.Println("Backup has been scheduled on another replset node")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	a.setBcp(&currentBackup{
		header: &bcp,
		cancel: cancel,
	})
	log.Printf("Backup %s started on node %s/%s", bcp.Name, nodeInfo.SetName, nodeInfo.Me)
	tstart := time.Now()
	err = backup.New(ctx, a.pbm, a.node).Run(bcp)
	a.unsetBcp()
	if err != nil {
		if errors.Is(err, backup.ErrCancelled) {
			log.Println("[INFO] backup was canceled")
		} else {
			log.Println("[ERROR] backup:", err)
		}
	} else {
		log.Printf("Backup %s finished", bcp.Name)
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
		log.Printf("[ERROR] backup: unable to release backup lock for %v:%v\n", lock, err)
	}
}

// Restore starts the restore
func (a *Agent) Restore(r pbm.RestoreCmd) {
	nodeInfo, err := a.node.GetIsMaster()
	if err != nil {
		log.Println("[ERROR] backup: get node isMaster data:", err)
		return
	}
	if !nodeInfo.IsMaster {
		log.Println("Node in not suitable for restore")
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
		log.Println("[ERROR] restore: acquiring lock:", err)
		return
	}
	if !got {
		log.Println("[ERROR] unbale to run the restore while another backup or restore process running")
		return
	}

	defer func() {
		err := lock.Release()
		if err != nil {
			log.Println("[ERROR] release lock:", err)
		}
	}()

	log.Printf("[INFO] Restore of '%s' started", r.BackupName)
	err = restore.New(a.pbm, a.node).Snapshot(r)
	if err != nil {
		log.Println("[ERROR] restore:", err)
		return
	}
	log.Printf("[INFO] Restore of '%s' finished successfully", r.BackupName)
}

// ResyncBackupList uploads a backup list from the remote store
func (a *Agent) ResyncBackupList() {
	nodeInfo, err := a.node.GetIsMaster()
	if err != nil {
		log.Println("[ERROR] resync_list: get node isMaster data:", err)
		return
	}

	if !nodeInfo.IsLeader() {
		log.Println("[INFO] resync_list: not a member of the leader rs")
		return
	}

	lock := a.pbm.NewLock(pbm.LockHeader{
		Type:    pbm.CmdResyncBackupList,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
	})

	got, err := lock.Acquire()
	if err != nil {
		switch err.(type) {
		case pbm.ErrConcurrentOp:
			log.Println("[INFO] resync_list: acquiring lock:", err)
		default:
			log.Println("[ERROR] resync_list: acquiring lock:", err)
		}
		return
	}
	if !got {
		log.Println("[INFO] resync_list: operation has been scheduled on another replset node")
		return
	}

	tstart := time.Now()
	log.Println("[INFO] resync_list: started")
	err = a.pbm.ResyncBackupList()
	if err != nil {
		log.Println("[ERROR] resync_list:", err)
	} else {
		log.Println("[INFO] resync_list: succeed")
	}

	needToWait := time.Second*1 - time.Since(tstart)
	if needToWait > 0 {
		time.Sleep(needToWait)
	}
	err = lock.Release()
	if err != nil {
		log.Printf("[ERROR] backup: unable to release backup lock for %v:%v\n", lock, err)
	}
}

func (a *Agent) aquireLock(l *pbm.Lock, m func(name string) error) (got bool, err error) {
	got, err = l.Acquire()
	if err == nil {
		return got, nil
	}

	switch err.(type) {
	case pbm.ErrConcurrentOp:
		log.Println("[INFO] backup: acquiring lock:", err)
		return false, nil
	case pbm.ErrWasStaleLock:
		if m != nil {
			name := err.(pbm.ErrWasStaleLock).Lock.BackupName
			merr := m(name)
			if merr != nil {
				log.Printf("[Warning] Failed to mark stale backup '%s' as failed: %v", name, merr)
			}
		}
		return l.Acquire()
	default:
		return false, err
	}
}
