package main

import (
	"context"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/election"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/version"
)

type currentBackup struct {
	cancel context.CancelFunc
}

func (a *Agent) setBcp(b *currentBackup) {
	a.mx.Lock()
	defer a.mx.Unlock()

	a.bcp = b
}

// CancelBackup cancels current backup
func (a *Agent) CancelBackup() {
	a.mx.Lock()
	defer a.mx.Unlock()

	if a.bcp == nil {
		return
	}

	a.bcp.cancel()
	a.bcp = nil
}

// Backup starts backup
func (a *Agent) Backup(ctx context.Context, cmd *ctrl.BackupCmd, opid ctrl.OPID, ep config.Epoch) {
	logger := log.FromContext(ctx)

	if cmd == nil {
		l := logger.NewEvent(string(ctrl.CmdBackup), "", opid.String(), ep.TS())
		l.Error("missed command")
		return
	}

	l := logger.NewEvent(string(ctrl.CmdBackup), cmd.Name, opid.String(), ep.TS())
	ctx = log.SetLogEventToContext(ctx, l)

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		l.Error("get node info: %v", err)
		return
	}
	// TODO: do the check on the agent start only
	if nodeInfo.IsStandalone() {
		l.Error("mongod node can not be used to fetch a consistent backup because it has no oplog. " +
			"Please restart it as a primary in a single-node replicaset " +
			"to make it compatible with PBM's backup method using the oplog")
		return
	}

	isClusterLeader := nodeInfo.IsClusterLeader()
	canRunBackup, err := topo.NodeSuitsExt(ctx, a.nodeConn, nodeInfo, cmd.Type)
	if err != nil {
		l.Error("node check: %v", err)
		if errors.Is(err, context.Canceled) || !isClusterLeader {
			return
		}
	}
	if !canRunBackup {
		l.Info("node is not suitable for backup")
		if !isClusterLeader {
			return
		}
	}

	if cmd.Type == defs.LogicalBackup {
		// wakeup the slicer to not wait for the tick
		go a.sliceNow(opid)
	}

	var bcp *backup.Backup
	switch cmd.Type {
	case defs.PhysicalBackup:
		bcp = backup.NewPhysical(a.leadConn, a.nodeConn, a.brief)
	case defs.ExternalBackup:
		bcp = backup.NewExternal(a.leadConn, a.nodeConn, a.brief)
	case defs.IncrementalBackup:
		bcp = backup.NewIncremental(a.leadConn, a.nodeConn, a.brief, cmd.IncrBase)
	case defs.LogicalBackup:
		fallthrough
	default:
		bcp = backup.New(a.leadConn, a.nodeConn, a.brief, a.dumpConns)
	}

	cfg, err := config.GetConfig(ctx, a.leadConn)
	if err != nil {
		l.Error("unable to get PBM config settings: " + err.Error())
		return
	}
	if storage.ParseType(string(cfg.Storage.Type)) == storage.Undef {
		l.Error("backups cannot be saved because PBM storage configuration hasn't been set yet")
		return
	}

	bcp.SetMongoVersion(a.mongoVersion.VersionString)
	bcp.SetSlicerInterval(cfg.BackupSlicerInterval())
	bcp.SetTimeouts(cfg.Backup.Timeouts)

	if isClusterLeader {
		balancer := topo.BalancerModeOff
		if nodeInfo.IsSharded() {
			bs, err := topo.GetBalancerStatus(ctx, a.leadConn)
			if err != nil {
				l.Error("get balancer status: %v", err)
				return
			}
			if bs.IsOn() {
				balancer = topo.BalancerModeOn
			}
		}
		err = bcp.Init(ctx, cmd, opid, nodeInfo, cfg.Storage, balancer, l)
		if err != nil {
			l.Error("init meta: %v", err)
			return
		}
		l.Debug("init backup meta")

		if err = topo.CheckTopoForBackup(ctx, a.leadConn, cmd.Type); err != nil {
			ferr := backup.ChangeBackupState(a.leadConn, cmd.Name, defs.StatusError, err.Error())
			l.Info("mark backup as %s `%v`: %v", defs.StatusError, err, ferr)
			return
		}

		// Incremental backup history is stored by WiredTiger on the node
		// not replset. So an `incremental && not_base` backup should land on
		// the agent that made a previous (src) backup.
		const srcHostMultiplier = 3.0
		var c map[string]float64
		if cmd.Type == defs.IncrementalBackup && !cmd.IncrBase {
			src, err := backup.LastIncrementalBackup(ctx, a.leadConn)
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

		agents, err := topo.ListAgentStatuses(ctx, a.leadConn)
		if err != nil {
			l.Error("get agents list: %v", err)
			return
		}

		validCandidates := make([]topo.AgentStat, 0, len(agents))
		for _, s := range agents {
			if version.FeatureSupport(s.MongoVersion()).BackupType(cmd.Type) != nil {
				continue
			}

			validCandidates = append(validCandidates, s)
		}

		nodes, err := election.BcpNodesPriority(ctx, a.leadConn, c, validCandidates)
		if err != nil {
			l.Error("get nodes priority: %v", err)
			return
		}
		shards, err := topo.ClusterMembers(ctx, a.leadConn.MongoClient())
		if err != nil {
			l.Error("get cluster members: %v", err)
			return
		}

		for _, sh := range shards {
			go func(rs string) {
				if err := a.nominateRS(ctx, cmd.Name, rs, nodes.RS(rs)); err != nil {
					l.Error("nodes nomination error for %s: %v", rs, err)
				}
			}(sh.RS)
		}
	}

	nominated, err := a.waitNomination(ctx, cmd.Name, nodeInfo.SetName, nodeInfo.Me)
	if err != nil {
		l.Error("wait for nomination: %v", err)
	}
	if !nominated {
		l.Debug("skip after nomination, probably started by another node")
		return
	}

	epoch := ep.TS()
	lck := lock.NewLock(a.leadConn, lock.LockHeader{
		Type:    ctrl.CmdBackup,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
		OPID:    opid.String(),
		Epoch:   &epoch,
	})

	got, err := a.acquireLock(ctx, lck, l)
	if err != nil {
		l.Error("acquiring lock: %v", err)
		return
	}
	if !got {
		l.Debug("skip: lock not acquired")
		return
	}
	defer func() {
		l.Debug("releasing lock")
		err = lck.Release()
		if err != nil {
			l.Error("unable to release backup lock %v: %v", lck, err)
		}
	}()

	err = backup.SetRSNomineeACK(ctx, a.leadConn, cmd.Name, nodeInfo.SetName, nodeInfo.Me)
	if err != nil {
		l.Warning("set nominee ack: %v", err)
	}

	bcpCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	a.setBcp(&currentBackup{cancel: cancel})
	defer a.setBcp(nil)

	l.Info("backup started")
	err = bcp.Run(bcpCtx, cmd, opid, l)
	if err != nil {
		if errors.Is(err, storage.ErrCancelled) || errors.Is(err, context.Canceled) {
			l.Info("backup was canceled")
		} else {
			l.Error("backup: %v", err)
		}
	} else {
		l.Info("backup finished")
	}
}

const renominationFrame = 5 * time.Second

func (a *Agent) nominateRS(ctx context.Context, bcp, rs string, nodes [][]string) error {
	l := log.LogEventFromContext(ctx)
	l.Debug("nomination list for %s: %v", rs, nodes)

	err := backup.SetRSNomination(ctx, a.leadConn, bcp, rs)
	if err != nil {
		return errors.Wrap(err, "set nomination meta")
	}

	for _, n := range nodes {
		nms, err := backup.GetRSNominees(ctx, a.leadConn, bcp, rs)
		if err != nil && !errors.Is(err, errors.ErrNotFound) {
			return errors.Wrap(err, "get nomination meta")
		}
		if nms != nil && len(nms.Ack) > 0 {
			l.Debug("bcp nomination: %s won by %s", rs, nms.Ack)
			return nil
		}

		err = backup.SetRSNominees(ctx, a.leadConn, bcp, rs, n)
		if err != nil {
			return errors.Wrap(err, "set nominees")
		}
		l.Debug("nomination %s, set candidates %v", rs, n)

		err = backup.BackupHB(ctx, a.leadConn, bcp)
		if err != nil {
			l.Warning("send heartbeat: %v", err)
		}

		time.Sleep(renominationFrame)
	}

	return nil
}

func (a *Agent) waitNomination(ctx context.Context, bcp, rs, node string) (bool, error) {
	l := log.LogEventFromContext(ctx)

	tk := time.NewTicker(time.Millisecond * 500)
	defer tk.Stop()

	stop := time.NewTimer(defs.WaitActionStart)
	defer stop.Stop()

	for {
		select {
		case <-tk.C:
			nm, err := backup.GetRSNominees(ctx, a.leadConn, bcp, rs)
			if err != nil {
				if errors.Is(err, errors.ErrNotFound) {
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
