package agent

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/log"
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
func (a *Agent) Backup(cmd *pbm.BackupCmd, opid pbm.OPID, ep pbm.Epoch) {
	if cmd == nil {
		l := a.log.NewEvent(string(pbm.CmdBackup), "", opid.String(), ep.TS())
		l.Error("missed command")
		return
	}

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

	// wakeup the slicer not to wait for the tick
	if p := a.getPitr(); p != nil {
		p.w <- &opid
	}

	var bcp *backup.Backup

	switch cmd.Type {
	case pbm.PhysicalBackup:
		bcp = backup.NewPhysical(a.pbm, a.node)
	case pbm.ExternalBackup:
		bcp = backup.NewExternal(a.pbm, a.node)
	case pbm.IncrementalBackup:
		bcp = backup.NewIncremental(a.pbm, a.node, cmd.IncrBase)
	case pbm.LogicalBackup:
		fallthrough
	default:
		bcp = backup.New(a.pbm, a.node)
	}

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
		err = bcp.Init(cmd, opid, nodeInfo, balancer)
		if err != nil {
			l.Error("init meta: %v", err)
			return
		}
		l.Debug("init backup meta")

		// Incremental backup history is stored by WiredTiger on the node
		// not replset. So an `incremental && not_base` backup should land on
		// the agent that made a previous (src) backup.
		const srcHostMultiplier = 3.0
		var c map[string]float64
		if cmd.Type == pbm.IncrementalBackup && !cmd.IncrBase {
			src, err := a.pbm.LastIncrementalBackup()
			if err != nil {
				// try backup anyway
				l.Warning("define source backup: %v", err)
			} else {
				c = make(map[string]float64)
				for _, rs := range src.Replsets {
					c[rs.Node] = srcHostMultiplier
				}
			}
		}
		nodes, err := a.pbm.BcpNodesPriority(c)
		if err != nil {
			l.Error("get nodes priority: %v", err)
			return
		}
		shards, err := a.pbm.ClusterMembers()
		if err != nil {
			l.Error("get cluster members: %v", err)
			return
		}
		for _, sh := range shards {
			go func(rs string) {
				err := a.nominateRS(cmd.Name, rs, nodes.RS(rs), l)
				if err != nil {
					l.Error("nodes nomination for %s: %v", rs, err)
				}
			}(sh.RS)
		}
	}

	nominated, err := a.waitNomination(cmd.Name, nodeInfo.SetName, nodeInfo.Me, l)
	if err != nil {
		l.Error("wait for nomination: %v", err)
	}

	if !nominated {
		l.Debug("skip after nomination, probably started by another node")
		return
	}

	epoch := ep.TS()
	lock := a.pbm.NewLock(pbm.LockHeader{
		Type:    pbm.CmdBackup,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
		OPID:    opid.String(),
		Epoch:   &epoch,
	})

	// install a backup lock despite having PITR one
	got, err := a.acquireLock(lock, l, func() (bool, error) {
		return lock.Rewrite(&pbm.LockHeader{
			Replset: a.node.RS(),
			Type:    pbm.CmdPITR,
		})
	})
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
		header: cmd,
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
	}

	l.Debug("releasing lock")
	err = lock.Release()
	if err != nil {
		l.Error("unable to release backup lock %v: %v", lock, err)
	}
}

const renominationFrame = 5 * time.Second

func (a *Agent) nominateRS(bcp, rs string, nodes [][]string, l *log.Event) error {
	l.Debug("nomination list for %s: %v", rs, nodes)
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
		l.Debug("nomination %s, set candidates %v", rs, n)

		err = a.pbm.BackupHB(bcp)
		if err != nil {
			l.Warning("send heartbeat: %v", err)
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
