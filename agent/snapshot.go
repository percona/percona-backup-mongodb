package agent

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/log"
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
func (a *Agent) Backup(cmd pbm.BackupCmd, opid pbm.OPID, ep pbm.Epoch) {
	l := a.log.NewEvent(string(pbm.CmdBackup), cmd.Name, opid.String(), ep.TS())

	nodeInfo, err := a.node.GetInfo()
	if err != nil {
		l.Error("get node info: %v", err)
		return
	}
	if nodeInfo.IsStandalone() {
		l.Error("mongod node can not be used to fetch a consistent backup because it has no oplog. Please restart it as a primary in a single-node replicaset to make it compatible with PBM's backup method using the oplog")
		return
	}

	// workaround for pitr
	//
	// 1. set a backup's intent so the PITR routine won't try to proceed, hence acquire a lock
	// 2. wait at least for pitrCheckPeriod * 1.1 to be sure PITR routine observed the state
	// 3. remove PITR lock and wake up PITR backup process so it may finish its PITR-stuff
	//    (nothing gonna be done if no PITR process run by current agent)
	// 4. try to acquire a backup's lock and move further with the backup
	// 5. despite any further plot development - clear the backup's intent to unblock PITR routine before return
	// TODO: this communication should be via db
	const waitForPITR = pitrCheckPeriod * 11 / 10
	atomic.StoreUint32(&a.intent, intentBackup)
	iat := time.Now()
	defer func() {
		time.Sleep(waitForPITR - time.Since(iat))
		atomic.StoreUint32(&a.intent, intentNone)
	}()

	q, err := backup.NodeSuits(a.node, nodeInfo)
	if err != nil {
		l.Error("node check: %v", err)
		return
	}

	// node is not suitable for doing backup
	if !q {
		l.Info("node is not suitable for backup")
		return
	}

	// before removing the lock wait if it's needed for
	// the PITR routine to observe a bcp intent
	if w := waitForPITR - time.Since(iat); w > 0 {
		time.Sleep(w)
	}

	err = a.pbm.NewLock(pbm.LockHeader{
		Type:    pbm.CmdPITR,
		Replset: nodeInfo.SetName,
	}).Release()
	if err != nil {
		l.Warning("clearing pitr locks: %v", err)
	}
	// wakeup the slicer not to wait for the tick
	a.wakeupPitr()

	bcp := backup.New(a.pbm, a.node)
	if nodeInfo.IsClusterLeader() {
		balancer := pbm.BalancerModeOff
		if nodeInfo.IsSharded() {
			bs, err := a.pbm.GetBalancerStatus()
			if err != nil {
				l.Error("get balancer status: %v", err)
				return
			}
			if bs.IsOn() {
				balancer = pbm.BalancerModeOn
			}
		}
		err = bcp.Init(cmd, opid, balancer)
		if err != nil {
			l.Error("init meta: %v", err)
			return
		}
		nodes, err := a.pbm.BcpNodesPriority()
		if err != nil {
			l.Error("get nodes priority: %v", err)
			return
		}
		shrds, err := a.pbm.ClusterMembers(nodeInfo)
		if err != nil {
			l.Error("get cluster members: %v", err)
			return
		}
		for _, sh := range shrds {
			go func(rs string) {
				err := a.nominateRS(cmd.Name, rs, nodes.RS(rs), l)
				if err != nil {
					l.Error("nodes nominaition for %s: %v", rs, err)
				}
			}(sh.RS)
		}
	}

	gotn, err := a.waitNomination(cmd.Name, nodeInfo.SetName, nodeInfo.Me, l)
	if err != nil {
		l.Error("wait for nomination: %v", err)
	}

	if !gotn {
		l.Debug("skip after nomination, probably started by another node")
		return
	}

	epts := ep.TS()
	lock := a.pbm.NewLock(pbm.LockHeader{
		Type:    pbm.CmdBackup,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
		OPID:    opid.String(),
		Epoch:   &epts,
	})

	got, err := a.aquireLock(lock)
	if err != nil {
		l.Error("acquiring lock: %v", err)
		return
	}
	if !got {
		l.Debug("skip: lock not acquired")
		return
	}

	err = a.pbm.SetRSNomineeACK(cmd.Name, nodeInfo.SetName, nodeInfo.Me)
	if err != nil {
		l.Warning("set nominee ack: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	a.setBcp(&currentBackup{
		header: &cmd,
		cancel: cancel,
	})
	l.Info("backup started")
	err = bcp.Run(ctx, cmd, opid, l)
	a.unsetBcp()
	if err != nil {
		if errors.Is(err, backup.ErrCancelled) {
			l.Info("backup was canceled")
		} else {
			l.Error("backup: %v", err)
		}
	} else {
		l.Info("backup finished")

		// Update PITR "changed" option to "reset" observation by pbm list of
		// any PITR related errors in the log since some of the errors might
		// be fixed by the backup. If not (errors wasn't fixed by bcp) PITR will
		// generate new errors so we won't miss anything
		if nodeInfo.IsLeader() {
			epch, err := a.pbm.ResetEpoch()
			if err != nil {
				l.Error("reset epoch")
			} else {
				l.Debug("epoch set to %v", epch)
			}
		}
	}

	l.Debug("releasing lock")
	err = lock.Release()
	if err != nil {
		l.Error("unable to release backup lock %v: %v", lock, err)
	}
}

const renominationFrame = 5 * time.Second

func (a *Agent) nominateRS(bcp, rs string, nodes [][]string, l *log.Event) error {
	l.Debug("nomination %s: %v", rs, nodes)
	err := a.pbm.SetRSNomination(bcp, rs)
	if err != nil {
		return errors.Wrap(err, "set nomination meta")
	}

	for _, n := range nodes {
		nms, err := a.pbm.GetRSNominees(bcp, rs)
		if err != nil && err != pbm.ErrNotFound {
			return errors.Wrap(err, "get nomination meta")
		}
		if nms != nil && len(nms.Ack) > 0 {
			l.Debug("bcp nomination: %s won by %s", rs, nms.Ack)
			return nil
		}

		err = a.pbm.SetRSNominees(bcp, rs, n)
		if err != nil {
			return errors.Wrap(err, "set nominees")
		}

		time.Sleep(renominationFrame)
	}

	return nil
}

func (a *Agent) waitNomination(bcp, rs, node string, l *log.Event) (got bool, err error) {
	tk := time.NewTicker(time.Millisecond * 500)
	defer tk.Stop()
	stop := time.NewTimer(pbm.WaitActionStart)
	defer stop.Stop()

	for {
		select {
		case <-tk.C:
			nm, err := a.pbm.GetRSNominees(bcp, rs)
			if err != nil {
				if errors.Is(err, pbm.ErrNotFound) {
					continue
				}
				return false, errors.Wrap(err, "check nomination")
			}
			if len(nm.Ack) > 0 {
				return false, nil
			}
			for _, n := range nm.Nodes {
				if n == node {
					return true, nil
				}
			}
		case <-stop.C:
			l.Debug("nomination timeout")
			return false, nil
		}
	}
}

// Restore starts the restore
func (a *Agent) Restore(r pbm.RestoreCmd, opid pbm.OPID, ep pbm.Epoch) {
	l := a.log.NewEvent(string(pbm.CmdRestore), r.BackupName, opid.String(), ep.TS())

	nodeInfo, err := a.node.GetInfo()
	if err != nil {
		l.Error("get node info: %v", err)
		return
	}
	if !nodeInfo.IsPrimary {
		l.Info("node is not primary so it unsuitable to do restore")
		return
	}

	epts := ep.TS()
	lock := a.pbm.NewLock(pbm.LockHeader{
		Type:    pbm.CmdRestore,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
		OPID:    opid.String(),
		Epoch:   &epts,
	})

	got, err := a.aquireLock(lock)
	if err != nil {
		l.Error("acquiring lock: %v", err)
		return
	}
	if !got {
		l.Debug("skip: lock not acquired")
		l.Error("unbale to run the restore while another operation running")
		return
	}

	defer func() {
		l.Debug("releasing lock")
		err := lock.Release()
		if err != nil {
			l.Error("release lock: %v", err)
		}
	}()

	l.Info("restore started")
	err = restore.New(a.pbm, a.node).Snapshot(r, opid, l)
	if err != nil {
		if errors.Is(err, restore.ErrNoDatForShard) {
			l.Info("no data for the shard in backup, skipping")
		} else {
			l.Error("restore: %v", err)
		}
		return
	}
	l.Info("restore finished successfully")

	if nodeInfo.IsLeader() {
		epch, err := a.pbm.ResetEpoch()
		if err != nil {
			l.Error("reset epoch")
			return
		}

		l.Debug("epoch set to %v", epch)
	}
}
