package agent

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/pitr"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"

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
	op      uint32
}

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
			log.Println("[ERROR] PITR: ", err)
		}
	}
}

func (a *Agent) pitr() (err error) {
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

	// extra check before real locking
	// just trying to avoid redundant heavy operations
	ts, err := a.pbm.ClusterTime()
	if err != nil {
		return errors.Wrap(err, "read cluster time")
	}
	tl, err := a.pbm.GetLockData(&pbm.LockHeader{Replset: nodeInfo.SetName}, pbm.LockCollection)
	// ErrNoDocuments or stale lock the only reasons to continue
	if err != mongo.ErrNoDocuments && tl.Heartbeat.T+pbm.StaleFrameSec >= ts.T {
		if err != nil {
			return errors.Wrap(err, "check is run")
		}
		return nil
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
	releasLock := func() {
		err := lock.Release()
		if err != nil {
			log.Println("[ERROR] PITR: release lock:", err)
		}
	}

	defer func() {
		// release lock only on error.
		// no error means the streaming go-routine was started
		// and it will release the lock when it finishes
		if err != nil {
			releasLock()
		}
	}()

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

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		w := make(chan struct{})

		a.setPitr(&currentPitr{
			cancel: cancel,
			wakeup: w,
		})

		err := ibcp.Stream(ctx, w, stg, pbm.CompressionTypeS2)
		if err != nil {
			log.Println("[ERROR] PITR: streaming oplog:", err)
		}

		a.unsetPitr()
		releasLock()
	}()

	return nil
}

// Start starts listening the commands stream.
func (a *Agent) Start() error {
	nodeInfo, err := a.node.GetIsMaster()
	if err != nil {
		log.Println("[Warning] get node isMaster data:", err)
	}
	log.Println("node:", nodeInfo.Me)

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
	// if pitr is on - remove the pitr's lock and aquire the backup's lock
	// and wakeup pitr routine to make the slice right up to the bcp start.
	// but before remove/aquire lock we should switch it off the pitr in the config
	// and wait for pitrCheckPeriod * 1.2
	// to prevent another node from aquiring a new pitr's lock before we got a backup's one.
	// after a backup's lock was aquired (or not) the pitr state should be restored
	pitrON, err := a.pbm.IsPITR()
	if err != nil {
		log.Println("[ERROR] backup: check if pitr is on:", err)
		return
	}
	restorePitr := func() {}
	if pitrON {
		restorePitr = func() {
			err := a.pbm.SetConfigVar("pitr.enabled", "true")
			if err != nil {
				log.Println("[ERROR] backup: unpause pitr:", err)
			}
		}
	}

	err = a.pbm.SetConfigVar("pitr.enabled", "false")
	if err != nil {
		log.Println("[ERROR] backup: pause pitr:", err)
		return
	}

	time.Sleep(pitrCheckPeriod * 12 / 10)

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
	restorePitr()
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
	err = restore.New(a.pbm, a.node).Run(r)
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
