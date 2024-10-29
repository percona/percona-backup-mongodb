package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/util"
	"github.com/percona/percona-backup-mongodb/pbm/version"
)

type Agent struct {
	leadConn connect.Client
	nodeConn *mongo.Client
	bcp      *currentBackup
	pitrjob  *currentPitr
	slicerMx sync.Mutex
	bcpMx    sync.Mutex

	brief topo.NodeBrief

	numParallelColls int

	closeCMD chan struct{}
	pauseHB  int32

	monMx sync.Mutex
	// signal for stopping pitr monitor jobs and flag that jobs are started/stopped
	monStopSig chan struct{}
}

func newAgent(
	ctx context.Context,
	leadConn connect.Client,
	uri string,
	numParallelColls int,
) (*Agent, error) {
	nodeConn, err := connect.MongoConnect(ctx, uri, connect.Direct(true))
	if err != nil {
		return nil, err
	}

	info, err := topo.GetNodeInfo(ctx, nodeConn)
	if err != nil {
		return nil, errors.Wrap(err, "get node info")
	}

	// set global values for replset name and node id ("host:port").
	// the values cannot be changed without restarting mongod
	defs.SetNodeBrief(info.SetName, info.Me)

	mongoVersion, err := version.GetMongoVersion(ctx, nodeConn)
	if err != nil {
		return nil, errors.Wrap(err, "get mongo version")
	}

	a := &Agent{
		leadConn: leadConn,
		closeCMD: make(chan struct{}),
		nodeConn: nodeConn,
		brief: topo.NodeBrief{
			URI:       uri,
			Sharded:   info.IsSharded(),
			ConfigSvr: info.IsConfigSrv(),
			Version:   mongoVersion,
		},
		numParallelColls: numParallelColls,
	}
	return a, nil
}

var (
	ErrArbiterNode = errors.New("arbiter")
	ErrDelayedNode = errors.New("delayed")
)

func (a *Agent) CanStart(ctx context.Context) error {
	info, err := topo.GetNodeInfo(ctx, a.nodeConn)
	if err != nil {
		return errors.Wrap(err, "get node info")
	}

	if info.IsStandalone() {
		return errors.New("mongod node can not be used to fetch a consistent " +
			"backup because it has no oplog. Please restart it as a primary " +
			"in a single-node replicaset to make it compatible with PBM's " +
			"backup method using the oplog")
	}
	if info.Msg == "isdbgrid" {
		return errors.New("mongos is not supported")
	}
	if info.ArbiterOnly {
		return ErrArbiterNode
	}
	if info.IsDelayed() {
		return ErrDelayedNode
	}

	return nil
}

func (a *Agent) showIncompatibilityWarning(ctx context.Context) {
	if err := version.FeatureSupport(a.brief.Version).PBMSupport(); err != nil {
		log.Warn(ctx, "WARNING: %v", err)
	}

	if a.brief.Sharded && a.brief.Version.IsShardedTimeseriesSupported() {
		tss, err := topo.ListShardedTimeseries(ctx, a.leadConn)
		if err != nil {
			log.Error(ctx, "failed to list sharded timeseries: %v", err)
		} else if len(tss) != 0 {
			log.Warn(ctx,
				"WARNING: cannot backup following sharded timeseries: %s",
				strings.Join(tss, ", "))
		}
	}

	if a.brief.Sharded && a.brief.Version.IsConfigShardSupported() {
		hasConfigShard, err := topo.HasConfigShard(ctx, a.leadConn)
		if err != nil {
			log.Error(ctx, "failed to check for Config Shard: %v", err)
		} else if hasConfigShard {
			log.Warn(ctx,
				"WARNING: selective backup and restore is not supported with Config Shard")
		}
	}
}

// Start starts listening the commands stream.
func (a *Agent) Start(ctx context.Context) error {
	log.Info(ctx, "pbm-agent:\n%s", version.Current().All(""))
	log.Info(ctx, "node: %s/%s", defs.Replset(), defs.NodeID())
	log.Info(ctx, "conn level ReadConcern: %v; WriteConcern: %v",
		a.leadConn.MongoOptions().ReadConcern.Level,
		a.leadConn.MongoOptions().WriteConcern.W)

	c, cerr := ctrl.ListenCmd(ctx, a.leadConn, a.closeCMD)

	log.Info(ctx, "listening for the commands")

	for {
		select {
		case cmd, ok := <-c:
			if !ok {
				log.Info(ctx, "change stream was closed")
				return nil
			}

			logAttrs := &log.Attrs{
				Event: string(cmd.Cmd),
				OPID:  cmd.OPID.String(),
				Name:  extractNameAttrFromCommand(&cmd),
			}

			log.InfoAttrs(logAttrs, "got command %s, opid: %s", cmd, cmd.OPID)

			ep, err := config.GetEpoch(ctx, a.leadConn)
			if err != nil {
				if !errors.Is(err, mongo.ErrNoDocuments) {
					log.ErrorAttrs(logAttrs, "get epoch: %v", err)
					continue
				}

				if cmd.Cmd != ctrl.CmdApplyConfig {
					log.ErrorAttrs(logAttrs, "cannot run command without PBM config")
					continue
				} else {
					log.WarnAttrs(logAttrs, "no config set")
				}
			}

			logAttrs.Epoch = ep.TS()
			cmdCtx := log.Context(ctx, log.WithAttrs(logAttrs))

			log.Debug(cmdCtx, "got epoch %v", ep)

			switch cmd.Cmd {
			case ctrl.CmdBackup:
				// backup runs in the go-routine so it can be canceled
				go a.Backup(cmdCtx, cmd.Backup, cmd.OPID, ep)
			case ctrl.CmdCancelBackup:
				a.CancelBackup()
			case ctrl.CmdRestore:
				a.Restore(cmdCtx, cmd.Restore, cmd.OPID, ep)
			case ctrl.CmdReplay:
				a.OplogReplay(cmdCtx, cmd.Replay, cmd.OPID, ep)
			case ctrl.CmdApplyConfig:
				a.handleApplyConfig(cmdCtx, cmd.Config, cmd.OPID, ep)
			case ctrl.CmdAddConfigProfile:
				a.handleAddConfigProfile(cmdCtx, cmd.Profile, cmd.OPID, ep)
			case ctrl.CmdRemoveConfigProfile:
				a.handleRemoveConfigProfile(cmdCtx, cmd.Profile, cmd.OPID, ep)
			case ctrl.CmdResync:
				a.Resync(cmdCtx, cmd.Resync, cmd.OPID, ep)
			case ctrl.CmdDeleteBackup:
				a.Delete(cmdCtx, cmd.Delete, cmd.OPID, ep)
			case ctrl.CmdDeletePITR:
				a.DeletePITR(cmdCtx, cmd.DeletePITR, cmd.OPID, ep)
			case ctrl.CmdCleanup:
				a.Cleanup(cmdCtx, cmd.Cleanup, cmd.OPID, ep)
			}
		case err, ok := <-cerr:
			if !ok {
				log.Info(ctx, "change stream was closed")
				return nil
			}

			if errors.Is(err, ctrl.CursorClosedError{}) {
				return errors.Wrap(err, "stop listening")
			}

			ep, _ := config.GetEpoch(ctx, a.leadConn)
			log.Error(log.Context(ctx, log.WithEpoch(ep.TS())), "listening commands: %v", err)
		}
	}
}

func extractNameAttrFromCommand(cmd *ctrl.Cmd) string {
	switch cmd.Cmd {
	case ctrl.CmdBackup:
		if cmd.Backup != nil {
			return cmd.Backup.Name
		}
	case ctrl.CmdRestore:
		if cmd.Restore != nil {
			return cmd.Restore.Name
		}
	case ctrl.CmdReplay:
		if cmd.Replay != nil {
			return cmd.Replay.Name
		}
	case ctrl.CmdAddConfigProfile, ctrl.CmdRemoveConfigProfile:
		if cmd.Profile != nil {
			return cmd.Profile.Name
		}
	case ctrl.CmdResync:
		if cmd.Resync != nil {
			return cmd.Resync.Name
		}
	case ctrl.CmdDeleteBackup:
		if cmd.Delete != nil {
			if cmd.Delete.Name != "" {
				return cmd.Delete.Name
			} else if cmd.Delete.OlderThan > 0 {
				t := time.Unix(cmd.DeletePITR.OlderThan, 0).UTC()
				return t.Format("2006-01-02T15:04:05Z")
			}
		}
	case ctrl.CmdDeletePITR:
		if cmd.DeletePITR != nil {
			t := time.Unix(cmd.DeletePITR.OlderThan, 0).UTC()
			return t.Format("2006-01-02T15:04:05Z")
		}
	case ctrl.CmdCancelBackup:
		// nothing
	case ctrl.CmdCleanup:
		// nothing
	}

	return ""
}

// acquireLock tries to acquire the lock. If there is a stale lock
// it tries to mark op that held the lock (backup, [pitr]restore) as failed.
func (a *Agent) acquireLock(ctx context.Context, l *lock.Lock) (bool, error) {
	got, err := l.Acquire(ctx)
	if err == nil {
		return got, nil
	}

	if errors.Is(err, lock.DuplicatedOpError{}) || errors.Is(err, lock.ConcurrentOpError{}) {
		log.Debug(ctx, "get lock: %v", err)
		return false, nil
	}

	var er lock.StaleLockError
	if !errors.As(err, &er) {
		return false, err
	}

	lck := er.Lock
	log.Debug(ctx, "stale lock: %v", lck)
	var fn func(context.Context, string) error
	switch lck.Type {
	case ctrl.CmdBackup:
		fn = func(ctx context.Context, opid string) error {
			ctx = log.Context(ctx,
				log.WithEvent(string(ctrl.CmdBackup)),
				log.WithOPID(lck.OPID))
			return markBcpStale(ctx, a.leadConn, opid)
		}
	case ctrl.CmdRestore:
		fn = func(ctx context.Context, opid string) error {
			ctx = log.Context(ctx,
				log.WithEvent(string(ctrl.CmdRestore)),
				log.WithOPID(lck.OPID))
			return markRestoreStale(ctx, a.leadConn, opid)
		}
	default:
		return l.Acquire(ctx)
	}

	if err := fn(ctx, lck.OPID); err != nil {
		log.Warn(ctx, "failed to mark stale op '%s' as failed: %v", lck.OPID, err)
	}

	return l.Acquire(ctx)
}

func (a *Agent) HbPause() {
	atomic.StoreInt32(&a.pauseHB, 1)
}

func (a *Agent) HbResume() {
	atomic.StoreInt32(&a.pauseHB, 0)
}

func (a *Agent) HbIsRun() bool {
	return atomic.LoadInt32(&a.pauseHB) == 0
}

func (a *Agent) HbStatus(ctx context.Context) {
	ctx = log.Context(ctx, log.WithEvent("agentCheckup"))

	nodeVersion, err := version.GetMongoVersion(ctx, a.nodeConn)
	if err != nil {
		log.Error(ctx, "get mongo version: %v", err)
	}

	hb := topo.AgentStat{
		Node:       defs.NodeID(),
		RS:         defs.Replset(),
		AgentVer:   version.Current().Version,
		MongoVer:   nodeVersion.VersionString,
		PerconaVer: nodeVersion.PSMDBVersion,
	}

	updateAgentStat(ctx, a, true, &hb)
	err = topo.SetAgentStatus(ctx, a.leadConn, &hb)
	if err != nil {
		log.Error(ctx, "set status: %v", err)
	}

	defer func() {
		log.Debug(ctx, "deleting agent status")
		err := topo.RemoveAgentStatus(context.Background(), a.leadConn, hb)
		if err != nil {
			log.Error(ctx, "remove agent heartbeat: %v", err)
		}
	}()

	tk := time.NewTicker(defs.AgentsStatCheckRange)
	defer tk.Stop()

	storageCheckTime := time.Now()
	parallelAgentCheckTime := time.Now()

	// check storage once in a while if all is ok (see https://jira.percona.com/browse/PBM-647)
	const storageCheckInterval = 15 * time.Second
	const parallelAgentCheckInternval = time.Minute

	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			// don't check if on pause (e.g. physical restore)
			if !a.HbIsRun() {
				continue // TODO: should be "return"
			}

			now := time.Now()
			if now.Sub(parallelAgentCheckTime) >= parallelAgentCheckInternval {
				a.warnIfParallelAgentDetected(ctx, hb.Heartbeat)
				parallelAgentCheckTime = now
			}

			if now.Sub(storageCheckTime) >= storageCheckInterval {
				updateAgentStat(ctx, a, true, &hb)
				err = topo.SetAgentStatus(ctx, a.leadConn, &hb)
				if err == nil {
					storageCheckTime = now
				}
			} else {
				updateAgentStat(ctx, a, false, &hb)
				err = topo.SetAgentStatus(ctx, a.leadConn, &hb)
			}
			if err != nil {
				log.Error(ctx, "set status: %v", err)
			}
		}
	}
}

func updateAgentStat(
	ctx context.Context,
	agent *Agent,
	checkStore bool,
	hb *topo.AgentStat,
) {
	hb.PBMStatus = agent.pbmStatus(ctx)
	if !hb.PBMStatus.OK {
		log.Error(ctx, "check PBM connection: %s", hb.PBMStatus.Err)
	}

	hb.NodeStatus = agent.nodeStatus(ctx)
	if !hb.NodeStatus.OK {
		log.Error(ctx, "check node connection: %s", hb.NodeStatus.Err)
	}

	hb.StorageStatus = agent.storStatus(ctx, checkStore, hb)
	if !hb.StorageStatus.OK {
		log.Error(ctx, "check storage connection: %s", hb.StorageStatus.Err)
	}

	hb.Err = ""
	hb.Hidden = false
	hb.Passive = false

	inf, err := topo.GetNodeInfo(ctx, agent.nodeConn)
	if err != nil {
		log.Error(ctx, "get NodeInfo: %v", err)
		hb.Err += fmt.Sprintf("get NodeInfo: %v", err)
	} else {
		hb.Hidden = inf.Hidden
		hb.Passive = inf.Passive
		hb.Arbiter = inf.ArbiterOnly
		if inf.SecondaryDelayOld != 0 {
			hb.DelaySecs = inf.SecondaryDelayOld
		} else {
			hb.DelaySecs = inf.SecondaryDelaySecs
		}

		hb.Heartbeat, err = topo.ClusterTimeFromNodeInfo(inf)
		if err != nil {
			hb.Err += fmt.Sprintf("get cluster time: %v", err)
		}
	}

	if inf != nil && inf.ArbiterOnly {
		hb.State = defs.NodeStateArbiter
		hb.StateStr = "ARBITER"
	} else {
		n, err := topo.GetNodeStatus(ctx, agent.nodeConn, defs.NodeID())
		if err != nil {
			log.Error(ctx, "get replSetGetStatus: %v", err)
			hb.Err += fmt.Sprintf("get replSetGetStatus: %v", err)
			hb.State = defs.NodeStateUnknown
			hb.StateStr = "UNKNOWN"
		} else {
			hb.State = n.State
			hb.StateStr = n.StateStr

			rLag, err := topo.ReplicationLag(ctx, agent.nodeConn, defs.NodeID())
			if err != nil {
				log.Error(ctx, "get replication lag: %v", err)
				hb.Err += fmt.Sprintf("get replication lag: %v", err)
			}
			hb.ReplicationLag = rLag
		}
	}
}

func (a *Agent) warnIfParallelAgentDetected(
	ctx context.Context,
	lastHeartbeat primitive.Timestamp,
) {
	s, err := topo.GetAgentStatus(ctx, a.leadConn, defs.Replset(), defs.NodeID())
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return
		}
		log.Error(ctx, "detecting parallel agent: get status: %v", err)
		return
	}
	if !s.Heartbeat.Equal(lastHeartbeat) {
		log.Warn(ctx, "detected possible parallel agent for the node: "+
			"expected last heartbeat to be %d.%d, actual is %d.%d",
			lastHeartbeat.T, lastHeartbeat.I, s.Heartbeat.T, s.Heartbeat.I)
		return
	}
}

func (a *Agent) pbmStatus(ctx context.Context) topo.SubsysStatus {
	err := a.leadConn.MongoClient().Ping(ctx, nil)
	if err != nil {
		return topo.SubsysStatus{Err: err.Error()}
	}

	return topo.SubsysStatus{OK: true}
}

func (a *Agent) nodeStatus(ctx context.Context) topo.SubsysStatus {
	err := a.nodeConn.Ping(ctx, nil)
	if err != nil {
		return topo.SubsysStatus{Err: err.Error()}
	}

	return topo.SubsysStatus{OK: true}
}

func (a *Agent) storStatus(
	ctx context.Context,
	forceCheckStorage bool,
	stat *topo.AgentStat,
) topo.SubsysStatus {
	// check storage once in a while if all is ok (see https://jira.percona.com/browse/PBM-647)
	// but if storage was(is) failed, check it always
	if !forceCheckStorage && stat.StorageStatus.OK {
		return topo.SubsysStatus{OK: true}
	}

	stg, err := util.GetStorage(ctx, a.leadConn, defs.NodeID())
	if err != nil {
		return topo.SubsysStatus{Err: fmt.Sprintf("unable to get storage: %v", err)}
	}

	ok, err := storage.IsInitialized(ctx, stg)
	if err != nil {
		errStr := fmt.Sprintf("storage check failed with: %v", err)
		return topo.SubsysStatus{Err: errStr}
	}
	if !ok {
		log.Warn(ctx, "storage is not initialized")
	}

	return topo.SubsysStatus{OK: true}
}
