package main

import (
	"context"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/prio"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/version"
)

type currentBackup struct {
	cancel context.CancelFunc
}

func (a *Agent) setBcp(b *currentBackup) {
	a.bcpMx.Lock()
	defer a.bcpMx.Unlock()

	a.bcp = b
}

// CancelBackup cancels current backup
func (a *Agent) CancelBackup() {
	a.bcpMx.Lock()
	defer a.bcpMx.Unlock()

	if a.bcp == nil {
		return
	}

	a.bcp.cancel()
	a.bcp = nil
}

// Backup starts backup
func (a *Agent) Backup(ctx context.Context, cmd *ctrl.BackupCmd, opid ctrl.OPID, ep config.Epoch) {
	if cmd == nil {
		log.Error(ctx, "missed command")
		return
	}

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		log.Error(ctx, "get node info: %v", err)
		return
	}
	if nodeInfo.ArbiterOnly {
		log.Debug(ctx, "arbiter node. skip")
		return
	}

	isClusterLeader := nodeInfo.IsClusterLeader()

	if isClusterLeader {
		moveOn, err := a.startBcpLockCheck(ctx)
		if err != nil {
			log.Error(ctx, "start backup lock check: %v", err)
			return
		}
		if !moveOn {
			log.Error(ctx, "unable to proceed with the backup, active lock is present")
			return
		}
	}

	canRunBackup, err := topo.NodeSuitsExt(ctx, a.nodeConn, nodeInfo, cmd.Type)
	if err != nil {
		log.Error(ctx, "node check: %v", err)
		if errors.Is(err, context.Canceled) || !isClusterLeader {
			return
		}
	}
	if !canRunBackup {
		log.Info(ctx, "node is not suitable for backup")
		if !isClusterLeader {
			return
		}
	}

	if cmd.Type == defs.LogicalBackup {
		// wakeup the slicer to not wait for the tick
		go a.sliceNow(opid)
	}

	cfg, err := config.GetProfiledConfig(ctx, a.leadConn, cmd.Profile)
	if err != nil {
		log.Error(ctx, "get profiled config: %v", err)
		return
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
		numParallelColls := a.numParallelColls
		if cfg.Backup != nil && cfg.Backup.NumParallelCollections > 0 {
			numParallelColls = cfg.Backup.NumParallelCollections
		}
		bcp = backup.New(a.leadConn, a.nodeConn, a.brief, numParallelColls)
	}

	bcp.SetConfig(cfg)
	bcp.SetMongoVersion(a.brief.Version.VersionString)
	bcp.SetSlicerInterval(cfg.BackupSlicerInterval())
	bcp.SetTimeouts(cfg.Backup.Timeouts)

	if isClusterLeader {
		balancer := topo.BalancerModeOff
		if a.brief.Sharded {
			bs, err := topo.GetBalancerStatus(ctx, a.leadConn)
			if err != nil {
				log.Error(ctx, "get balancer status: %v", err)
				return
			}
			if bs.IsOn() {
				balancer = topo.BalancerModeOn
			}
		}
		err = bcp.Init(ctx, cmd, opid, balancer)
		if err != nil {
			log.Error(ctx, "init meta: %v", err)
			return
		}
		log.Debug(ctx, "init backup meta")

		if err = topo.CheckTopoForBackup(ctx, a.leadConn, cmd.Type); err != nil {
			ferr := backup.ChangeBackupState(ctx,
				a.leadConn, cmd.Name, defs.StatusError, err.Error())
			// TODO: avoid suffix ": <nil>"
			log.Info(ctx, "mark backup as %s `%v`: %v", defs.StatusError, err, ferr)
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
				log.Warn(ctx, "define source backup: %v", err)
			} else {
				c = make(map[string]float64)
				for _, rs := range src.Replsets {
					c[rs.Node] = srcHostMultiplier
				}
			}
		}

		agents, err := topo.ListSteadyAgents(ctx, a.leadConn)
		if err != nil {
			log.Error(ctx, "get agents list: %v", err)
			return
		}

		candidates := a.getValidCandidates(agents, cmd.Type)

		nodes := prio.CalcNodesPriority(c, cfg.Backup.Priority, candidates)

		shards, err := topo.ClusterMembers(ctx, a.leadConn.MongoClient())
		if err != nil {
			log.Error(ctx, "get cluster members: %v", err)
			return
		}

		for _, sh := range shards {
			go func(rs string) {
				if err := a.nominateRS(ctx, cmd.Name, rs, nodes.RS(rs)); err != nil {
					log.Error(ctx, "nodes nomination error for %s: %v", rs, err)
				}
			}(sh.RS)
		}
	}

	nominated, err := a.waitNomination(ctx, cmd.Name)
	if err != nil {
		log.Error(ctx, "wait for nomination: %v", err)
	}
	if !nominated {
		log.Debug(ctx, "skip after nomination, probably started by another node")
		return
	}

	epoch := ep.TS()
	lck := lock.NewLock(a.leadConn, lock.LockHeader{
		Type:    ctrl.CmdBackup,
		Replset: defs.Replset(),
		Node:    defs.NodeID(),
		OPID:    opid.String(),
		Epoch:   &epoch,
	})

	got, err := a.acquireLock(ctx, lck)
	if err != nil {
		log.Error(ctx, "acquiring lock: %v", err)
		return
	}
	if !got {
		log.Debug(ctx, "skip: lock not acquired")
		return
	}
	defer func() {
		log.Debug(ctx, "releasing lock")
		err = lck.Release()
		if err != nil {
			log.Error(ctx, "unable to release backup lock %v: %v", lck, err)
		}
	}()

	err = backup.SetRSNomineeACK(ctx, a.leadConn, cmd.Name, nodeInfo.SetName, nodeInfo.Me)
	if err != nil {
		log.Warn(ctx, "set nominee ack: %v", err)
	}

	bcpCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	a.setBcp(&currentBackup{cancel: cancel})
	defer a.setBcp(nil)

	log.Info(ctx, "backup started")
	err = bcp.Run(bcpCtx, cmd, opid)
	if err != nil {
		if errors.Is(err, storage.ErrCancelled) || errors.Is(err, context.Canceled) {
			log.Info(ctx, "backup was canceled")
		} else {
			log.Error(ctx, "backup: %v", err)
		}
	} else {
		log.Info(ctx, "backup finished")
	}
}

// getValidCandidates filters out all agents that are not suitable for the backup.
func (a *Agent) getValidCandidates(agents []topo.AgentStat, backupType defs.BackupType) []topo.AgentStat {
	validCandidates := []topo.AgentStat{}
	for _, agent := range agents {
		if version.FeatureSupport(agent.MongoVersion()).BackupType(backupType) != nil {
			continue
		}
		validCandidates = append(validCandidates, agent)
	}

	return validCandidates
}

const renominationFrame = 5 * time.Second

func (a *Agent) nominateRS(ctx context.Context, bcp, rs string, nodes [][]string) error {
	log.Debug(ctx, "nomination list for %s: %v", rs, nodes)

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
			log.Debug(ctx, "bcp nomination: %s won by %s", rs, nms.Ack)
			return nil
		}

		err = backup.SetRSNominees(ctx, a.leadConn, bcp, rs, n)
		if err != nil {
			return errors.Wrap(err, "set nominees")
		}
		log.Debug(ctx, "nomination %s, set candidates %v", rs, n)

		err = backup.BackupHB(ctx, a.leadConn, bcp)
		if err != nil {
			log.Warn(ctx, "send heartbeat: %v", err)
		}

		time.Sleep(renominationFrame)
	}

	return nil
}

func (a *Agent) waitNomination(ctx context.Context, bcp string) (bool, error) {
	tk := time.NewTicker(time.Millisecond * 500)
	defer tk.Stop()

	stop := time.NewTimer(defs.WaitActionStart)
	defer stop.Stop()

	replsetName := defs.Replset()
	nodeID := defs.NodeID()

	for {
		select {
		case <-tk.C:
			nm, err := backup.GetRSNominees(ctx, a.leadConn, bcp, replsetName)
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
				if n == nodeID {
					return true, nil
				}
			}
		case <-stop.C:
			log.Debug(ctx, "nomination timeout")
			return false, nil
		}
	}
}

// startBcpLockCheck checks if there is any active lock.
// It fetches all existing pbm locks, and if any exists, it is also
// checked for staleness.
// false is returned in case a single active lock exists or error happens.
// true means that there's no active locks.
func (a *Agent) startBcpLockCheck(ctx context.Context) (bool, error) {
	locks, err := lock.GetLocks(ctx, a.leadConn, &lock.LockHeader{})
	if err != nil {
		return false, errors.Wrap(err, "get all locks for backup start")
	}
	if len(locks) == 0 {
		return true, nil
	}

	// stale lock check
	ts, err := topo.GetClusterTime(ctx, a.leadConn)
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	for _, l := range locks {
		if l.Heartbeat.T+defs.StaleFrameSec >= ts.T {
			return false, nil
		}
	}

	return true, nil
}
