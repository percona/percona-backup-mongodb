package main

import (
	"context"
	"fmt"
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
	"github.com/percona/percona-backup-mongodb/pbm/resync"
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
	mx       sync.Mutex

	brief topo.NodeBrief

	mongoVersion version.MongoVersion

	dumpConns int

	closeCMD chan struct{}
	pauseHB  int32

	// prevOO is previous pitr.oplogOnly value
	prevOO *bool
}

func newAgent(ctx context.Context, leadConn connect.Client, uri string, dumpConns int) (*Agent, error) {
	nodeConn, err := connect.MongoConnect(ctx, uri, connect.Direct(true))
	if err != nil {
		return nil, err
	}

	info, err := topo.GetNodeInfo(ctx, nodeConn)
	if err != nil {
		return nil, errors.Wrap(err, "get node info")
	}

	mongoVersion, err := version.GetMongoVersion(ctx, nodeConn)
	if err != nil {
		return nil, errors.Wrap(err, "get mongo version")
	}

	a := &Agent{
		leadConn: leadConn,
		closeCMD: make(chan struct{}),
		nodeConn: nodeConn,
		brief: topo.NodeBrief{
			URI:     uri,
			SetName: info.SetName,
			Me:      info.Me,
			Sharded: info.IsSharded(),
		},
		mongoVersion: mongoVersion,
		dumpConns:    dumpConns,
	}
	return a, nil
}

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

	ver, err := version.GetMongoVersion(ctx, a.leadConn.MongoClient())
	if err != nil {
		return errors.Wrap(err, "get mongo version")
	}
	if err := version.FeatureSupport(ver).PBMSupport(); err != nil {
		log.FromContext(ctx).
			Warning("", "", "", primitive.Timestamp{}, "WARNING: %v", err)
	}

	return nil
}

// Start starts listening the commands stream.
func (a *Agent) Start(ctx context.Context) error {
	logger := log.FromContext(ctx)
	logger.Printf("pbm-agent:\n%s", version.Current().All(""))
	logger.Printf("node: %s/%s", a.brief.SetName, a.brief.Me)
	logger.Printf("conn level ReadConcern: %v; WriteConcern: %v",
		a.leadConn.MongoOptions().ReadConcern.Level,
		a.leadConn.MongoOptions().WriteConcern.W)

	c, cerr := ctrl.ListenCmd(ctx, a.leadConn, a.closeCMD)

	logger.Printf("listening for the commands")

	for {
		select {
		case cmd, ok := <-c:
			if !ok {
				logger.Printf("change stream was closed")
				return nil
			}

			logger.Printf("got command %s", cmd)

			ep, err := config.GetEpoch(ctx, a.leadConn)
			if err != nil {
				logger.Error(string(cmd.Cmd), "", cmd.OPID.String(), ep.TS(), "get epoch: %v", err)
				continue
			}

			logger.Printf("got epoch %v", ep)

			switch cmd.Cmd {
			case ctrl.CmdBackup:
				// backup runs in the go-routine so it can be canceled
				go a.Backup(ctx, cmd.Backup, cmd.OPID, ep)
			case ctrl.CmdCancelBackup:
				a.CancelBackup()
			case ctrl.CmdRestore:
				a.Restore(ctx, cmd.Restore, cmd.OPID, ep)
			case ctrl.CmdReplay:
				a.OplogReplay(ctx, cmd.Replay, cmd.OPID, ep)
			case ctrl.CmdAddConfigProfile:
				a.handleAddConfigProfile(ctx, cmd.Profile, cmd.OPID, ep)
			case ctrl.CmdRemoveConfigProfile:
				a.handleRemoveConfigProfile(ctx, cmd.Profile, cmd.OPID, ep)
			case ctrl.CmdResync:
				a.Resync(ctx, cmd.Resync, cmd.OPID, ep)
			case ctrl.CmdDeleteBackup:
				a.Delete(ctx, cmd.Delete, cmd.OPID, ep)
			case ctrl.CmdDeletePITR:
				a.DeletePITR(ctx, cmd.DeletePITR, cmd.OPID, ep)
			case ctrl.CmdCleanup:
				a.Cleanup(ctx, cmd.Cleanup, cmd.OPID, ep)
			}
		case err, ok := <-cerr:
			if !ok {
				logger.Printf("change stream was closed")
				return nil
			}

			if errors.Is(err, ctrl.CursorClosedError{}) {
				return errors.Wrap(err, "stop listening")
			}

			ep, _ := config.GetEpoch(ctx, a.leadConn)
			logger.Error("", "", "", ep.TS(), "listening commands: %v", err)
		}
	}
}

// Resync uploads a backup list from the remote store
func (a *Agent) Resync(ctx context.Context, cmd *ctrl.ResyncCmd, opid ctrl.OPID, ep config.Epoch) {
	if cmd == nil {
		cmd = &ctrl.ResyncCmd{}
	}

	logger := log.FromContext(ctx)
	l := logger.NewEvent(string(ctrl.CmdResync), "", opid.String(), ep.TS())
	ctx = log.SetLogEventToContext(ctx, l)

	a.HbResume()
	logger.ResumeMgo()

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		l.Error("get node info data: %v", err)
		return
	}

	if !nodeInfo.IsLeader() {
		l.Info("not a member of the leader rs")
		return
	}

	lock := lock.NewLock(a.leadConn, lock.LockHeader{
		Type:    ctrl.CmdResync,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
		OPID:    opid.String(),
		Epoch:   util.Ref(ep.TS()),
	})

	got, err := a.acquireLock(ctx, lock, l)
	if err != nil {
		l.Error("acquiring lock: %v", err)
		return
	}
	if !got {
		l.Debug("lock not acquired")
		return
	}

	defer func() {
		if err := lock.Release(); err != nil {
			l.Error("release lock %v: %v", lock, err)
		}
	}()

	l.Info("started")

	if cmd.All {
		profiles, err := config.ListProfiles(ctx, a.leadConn)
		if err != nil {
			l.Error("get config profiles: %v", err)
			return
		}

		if cmd.Clear {
			l.Debug("clearing backup list for %d config profiles", len(profiles))
			for i := range profiles {
				name := profiles[i].Name
				err = resync.ClearBackupList(ctx, a.leadConn, name)
				if err != nil {
					l.Error("clear backup list for %q: %v", name, err)
				}
			}
		} else {
			l.Debug("syncing backup list for %d config profiles", len(profiles))
			for i := range profiles {
				profile := &profiles[i]
				err = resync.SyncBackupList(ctx, a.leadConn, &profile.Storage, profile.Name)
				if err != nil {
					l.Error("sync backup list for %q: %v", profile.Name, err)
					return
				}
			}
		}
	} else if cmd.Name != "" {
		profile, err := config.GetProfile(ctx, a.leadConn, cmd.Name)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				err = errors.Errorf("profile %q not found", cmd.Name)
			}

			l.Error("get config profile: %v", err)
			return
		}

		if cmd.Clear {
			l.Debug("clearing backup list for %q", profile.Name)
			err = resync.ClearBackupList(ctx, a.leadConn, profile.Name)
			if err != nil {
				l.Error("clear backup list for %q: %v", profile.Name, err)
			}
		} else {
			l.Debug("syncing backup list for %q", profile.Name)
			err = resync.SyncBackupList(ctx, a.leadConn, &profile.Storage, profile.Name)
			if err != nil {
				l.Error("sync backup list for %q: %v", profile.Name, err)
				return
			}
		}
	} else { // resync main storage only
		l.Debug("resync from main storage")
		cfg, err := config.GetConfig(ctx, a.leadConn)
		if err != nil {
			l.Error("get config: %v", err)
			return
		}

		err = resync.Resync(ctx, a.leadConn, &cfg.Storage)
		if err != nil {
			l.Error("resync: %v", err)
			return
		}

		epch, err := config.ResetEpoch(ctx, a.leadConn)
		if err != nil {
			l.Error("reset epoch: %v", err)
			return
		}
		l.Debug("epoch set to %v", epch)
	}

	l.Info("succeed")
}

// acquireLock tries to acquire the lock. If there is a stale lock
// it tries to mark op that held the lock (backup, [pitr]restore) as failed.
func (a *Agent) acquireLock(ctx context.Context, l *lock.Lock, lg log.LogEvent) (bool, error) {
	got, err := l.Acquire(ctx)
	if err == nil {
		return got, nil
	}

	if errors.Is(err, lock.DuplicatedOpError{}) || errors.Is(err, lock.ConcurrentOpError{}) {
		lg.Debug("get lock: %v", err)
		return false, nil
	}

	var er lock.StaleLockError
	if !errors.As(err, &er) {
		return false, err
	}

	lck := er.Lock
	lg.Debug("stale lock: %v", lck)
	var fn func(context.Context, *lock.Lock, string) error
	switch lck.Type {
	case ctrl.CmdBackup:
		fn = markBcpStale
	case ctrl.CmdRestore:
		fn = markRestoreStale
	default:
		return l.Acquire(ctx)
	}

	if err := fn(ctx, l, lck.OPID); err != nil {
		lg.Warning("failed to mark stale op '%s' as failed: %v", lck.OPID, err)
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
	logger := log.FromContext(ctx)
	l := logger.NewEvent("agentCheckup", "", "", primitive.Timestamp{})
	ctx = log.SetLogEventToContext(ctx, l)

	nodeVersion, err := version.GetMongoVersion(ctx, a.nodeConn)
	if err != nil {
		l.Error("get mongo version: %v", err)
	}

	hb := topo.AgentStat{
		Node:       a.brief.Me,
		RS:         a.brief.SetName,
		AgentVer:   version.Current().Version,
		MongoVer:   nodeVersion.VersionString,
		PerconaVer: nodeVersion.PSMDBVersion,
	}
	defer func() {
		if err := topo.RemoveAgentStatus(ctx, a.leadConn, hb); err != nil {
			logger := logger.NewEvent("agentCheckup", "", "", primitive.Timestamp{})
			logger.Error("remove agent heartbeat: %v", err)
		}
	}()

	tk := time.NewTicker(defs.AgentsStatCheckRange)
	defer tk.Stop()

	// check storage once in a while if all is ok (see https://jira.percona.com/browse/PBM-647)
	const checkStoreIn = int(60 / (defs.AgentsStatCheckRange / time.Second))
	cc := 0
	for range tk.C {
		// don't check if on pause (e.g. physical restore)
		if !a.HbIsRun() {
			continue
		}

		hb.PBMStatus = a.pbmStatus(ctx)
		logHbStatus("PBM connection", hb.PBMStatus, l)

		hb.NodeStatus = a.nodeStatus(ctx)
		logHbStatus("node connection", hb.NodeStatus, l)

		cc++
		hb.StorageStatus = a.storStatus(ctx, l, cc == checkStoreIn)
		logHbStatus("storage connection", hb.StorageStatus, l)
		if cc == checkStoreIn {
			cc = 0
		}

		hb.Err = ""

		hb.State = defs.NodeStateUnknown
		hb.StateStr = "unknown"
		n, err := topo.GetNodeStatus(ctx, a.nodeConn, a.brief.Me)
		if err != nil {
			l.Error("get replSetGetStatus: %v", err)
			hb.Err += fmt.Sprintf("get replSetGetStatus: %v", err)
		} else {
			hb.State = n.State
			hb.StateStr = n.StateStr
		}

		hb.Hidden = false
		hb.Passive = false

		inf, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
		if err != nil {
			l.Error("get NodeInfo: %v", err)
			hb.Err += fmt.Sprintf("get NodeInfo: %v", err)
		} else {
			hb.Hidden = inf.Hidden
			hb.Passive = inf.Passive
			hb.Arbiter = inf.ArbiterOnly
		}

		err = topo.SetAgentStatus(ctx, a.leadConn, hb)
		if err != nil {
			l.Error("set status: %v", err)
		}
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

func (a *Agent) storStatus(ctx context.Context, log log.LogEvent, forceCheckStorage bool) topo.SubsysStatus {
	// check storage once in a while if all is ok (see https://jira.percona.com/browse/PBM-647)
	// but if storage was(is) failed, check it always
	stat, err := topo.GetAgentStatus(ctx, a.leadConn, a.brief.SetName, a.brief.Me)
	if err != nil {
		log.Warning("get current storage status: %v", err)
	}
	if !forceCheckStorage && stat.StorageStatus.OK {
		return topo.SubsysStatus{OK: true}
	}

	stg, err := util.GetStorage(ctx, a.leadConn, log)
	if err != nil {
		return topo.SubsysStatus{Err: fmt.Sprintf("unable to get storage: %v", err)}
	}

	ok, err := storage.IsInitialized(ctx, stg)
	if err != nil {
		errStr := fmt.Sprintf("storage check failed with: %v", err)
		return topo.SubsysStatus{Err: errStr}
	}
	if !ok {
		log.Warning("storage is not initialized")
	}

	return topo.SubsysStatus{OK: true}
}

func logHbStatus(name string, st topo.SubsysStatus, l log.LogEvent) {
	if !st.OK {
		l.Error("check %s: %s", name, st.Err)
	}
}
