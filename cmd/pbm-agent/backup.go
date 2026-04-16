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
	"github.com/percona/percona-backup-mongodb/pbm/util"
	"github.com/percona/percona-backup-mongodb/pbm/version"
	"github.com/percona/percona-backup-mongodb/pbm/webhook"
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
	if nodeInfo.ArbiterOnly {
		l.Debug("arbiter node. skip")
		return
	}

	isClusterLeader := nodeInfo.IsClusterLeader()

	if isClusterLeader {
		moveOn, err := a.startBcpLockCheck(ctx)
		if err != nil {
			l.Error("start backup lock check: %v", err)
			return
		}
		if !moveOn {
			l.Error("unable to proceed with the backup, active lock is present")
			return
		}
	}

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

	if cmd.Type == defs.LogicalBackup && cmd.Profile == "" {
		// For backups to the main storage,
		// wake up the slicer to not wait for the tick.
		// This will slice and pause the main PITR
		go a.sliceNow(opid)
	}

	cfg, err := config.GetProfiledConfig(ctx, a.leadConn, cmd.Profile)
	if err != nil {
		l.Error("get profiled config: %v", err)
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
				l.Error("get balancer status: %v", err)
				return
			}
			if bs.IsOn() {
				balancer = topo.BalancerModeOn
			}
		}
		err = bcp.Init(ctx, cmd, opid, balancer)
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
		c := make(map[string]float64)
		if cmd.Type == defs.IncrementalBackup && !cmd.IncrBase {
			src, err := backup.LastIncrementalBackup(ctx, a.leadConn)
			if err != nil {
				// try backup anyway
				l.Warning("define source backup: %v", err)
			} else {
				for _, rs := range src.Replsets {
					c[rs.Node] = srcHostMultiplier
				}
			}
		}

		// When a logical backup targets an external profile (different storage),
		// PITR keeps running. Deprioritize nodes currently running PITR slicer
		if cmd.Type == defs.LogicalBackup && cmd.Profile != "" {
			c = a.deprioritizePITRNodes(ctx, c, l)
		}

		agents, err := topo.ListSteadyAgents(ctx, a.leadConn)
		if err != nil {
			l.Error("get agents list: %v", err)
			return
		}

		candidates := a.getValidCandidates(agents, cmd.Type)

		nodes := prio.CalcNodesPriority(c, cfg.Backup.Priority, candidates)

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

	nominated, err := a.waitNomination(ctx, cmd.Name)
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
		Replset: a.brief.SetName,
		Node:    a.brief.Me,
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

	wh := webhook.NewNotifier(ctx, cfg.Webhooks, l)

	l.Info("backup started %s", util.LogProfileArg(cmd.Profile))
	if nodeInfo.IsClusterLeader() {
		wh.SendBackup(webhook.EventStart, cmd.Name, nil)
	}

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

	if nodeInfo.IsClusterLeader() {
		if err != nil {
			wh.SendBackup(webhook.EventError, cmd.Name, err)
		} else {
			wh.SendBackup(webhook.EventDone, cmd.Name, nil)
		}
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

// deprioritizePITRNodes adds low-priority coefficients for nodes currently running PITR slicing.
// It only modifies the coefficient map for nodes not already present (e.g., incremental src host).
// Returns the (possibly modified) coefficient map.
func (a *Agent) deprioritizePITRNodes(
	ctx context.Context,
	coefficients map[string]float64,
	l log.LogEvent,
) map[string]float64 {
	pitrLocks, err := lock.GetOpLocks(ctx, a.leadConn, &lock.LockHeader{Type: ctrl.CmdPITR})
	if err != nil {
		l.Warning("get pitr locks for deprioritization: %v", err)
		return coefficients
	}

	ts, err := topo.GetClusterTime(ctx, a.leadConn)
	if err != nil {
		l.Warning("get cluster time for pitr deprioritization: %v", err)
		return coefficients
	}

	for i := range pitrLocks {
		pl := &pitrLocks[i]
		if pl.Heartbeat.T+defs.StaleFrameSec < ts.T {
			continue // stale lock, ignore
		}

		// Only set if not already present (preserve previous priorities)
		if _, exists := coefficients[pl.Node]; !exists {
			coefficients[pl.Node] = prio.DefaultScore - 0.1
		}
	}

	return coefficients
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

func (a *Agent) waitNomination(ctx context.Context, bcp string) (bool, error) {
	l := log.LogEventFromContext(ctx)

	tk := time.NewTicker(time.Millisecond * 500)
	defer tk.Stop()

	stop := time.NewTimer(defs.WaitActionStart)
	defer stop.Stop()

	for {
		select {
		case <-tk.C:
			nm, err := backup.GetRSNominees(ctx, a.leadConn, bcp, a.brief.SetName)
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
				if n == a.brief.Me {
					return true, nil
				}
			}
		case <-stop.C:
			l.Debug("nomination timeout")
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
