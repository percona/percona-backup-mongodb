package main

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/lock"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/resync"
	"github.com/percona/percona-backup-mongodb/internal/storage"
	"github.com/percona/percona-backup-mongodb/internal/topo"
	"github.com/percona/percona-backup-mongodb/internal/types"
	"github.com/percona/percona-backup-mongodb/internal/util"
	"github.com/percona/percona-backup-mongodb/internal/version"
	"github.com/percona/percona-backup-mongodb/pbm"
)

type Agent struct {
	pbm     *pbm.PBM
	node    *pbm.Node
	bcp     *currentBackup
	pitrjob *currentPitr
	mx      sync.Mutex
	log     *log.Logger

	closeCMD chan struct{}
	pauseHB  int32

	// prevOO is previous pitr.oplogOnly value
	prevOO *bool
}

func newAgent(pbm *pbm.PBM) *Agent {
	return &Agent{
		pbm:      pbm,
		closeCMD: make(chan struct{}),
	}
}

func (a *Agent) AddNode(ctx context.Context, curi string, dumpConns int) error {
	var err error
	a.node, err = pbm.NewNode(ctx, curi, dumpConns)
	return err
}

func (a *Agent) InitLogger() {
	a.pbm.InitLogger(a.node.RS(), a.node.Name())
	a.log = a.pbm.Logger()
}

func (a *Agent) Close() {
	if a.log != nil {
		a.log.Close()
	}
}

func (a *Agent) CanStart(ctx context.Context) error {
	info, err := topo.GetNodeInfoExt(ctx, a.node.Session())
	if err != nil {
		return errors.Wrap(err, "get node info")
	}

	if info.Msg == "isdbgrid" {
		return errors.New("mongos is not supported")
	}

	ver, err := version.GetMongoVersion(ctx, a.pbm.Conn.MongoClient())
	if err != nil {
		return errors.Wrap(err, "get mongo version")
	}
	if err := version.FeatureSupport(ver).PBMSupport(); err != nil {
		a.log.Warning("", "", "", primitive.Timestamp{}, "WARNING: %v", err)
	}

	return nil
}

// Start starts listening the commands stream.
func (a *Agent) Start(ctx context.Context) error {
	a.log.Printf("pbm-agent:\n%s", version.Current().All(""))
	a.log.Printf("node: %s", a.node.ID())

	c, cerr := ListenCmd(ctx, a.pbm.Conn, a.closeCMD)

	a.log.Printf("listening for the commands")

	for {
		select {
		case cmd, ok := <-c:
			if !ok {
				a.log.Printf("change stream was closed")
				return nil
			}

			a.log.Printf("got command %s", cmd)

			ep, err := config.GetEpoch(ctx, a.pbm.Conn)
			if err != nil {
				a.log.Error(string(cmd.Cmd), "", cmd.OPID.String(), ep.TS(), "get epoch: %v", err)
				continue
			}

			a.log.Printf("got epoch %v", ep)

			switch cmd.Cmd {
			case defs.CmdBackup:
				// backup runs in the go-routine so it can be canceled
				go a.Backup(ctx, cmd.Backup, cmd.OPID, ep)
			case defs.CmdCancelBackup:
				a.CancelBackup()
			case defs.CmdRestore:
				a.Restore(ctx, cmd.Restore, cmd.OPID, ep)
			case defs.CmdReplay:
				a.OplogReplay(ctx, cmd.Replay, cmd.OPID, ep)
			case defs.CmdResync:
				a.Resync(ctx, cmd.OPID, ep)
			case defs.CmdDeleteBackup:
				a.Delete(ctx, cmd.Delete, cmd.OPID, ep)
			case defs.CmdDeletePITR:
				a.DeletePITR(ctx, cmd.DeletePITR, cmd.OPID, ep)
			case defs.CmdCleanup:
				a.Cleanup(ctx, cmd.Cleanup, cmd.OPID, ep)
			}
		case err, ok := <-cerr:
			if !ok {
				a.log.Printf("change stream was closed")
				return nil
			}

			if errors.Is(err, cursorClosedError{}) {
				return errors.Wrap(err, "stop listening")
			}

			ep, _ := config.GetEpoch(ctx, a.pbm.Conn)
			a.log.Error("", "", "", ep.TS(), "listening commands: %v", err)
		}
	}
}

func ListenCmd(ctx context.Context, m connect.Client, cl <-chan struct{}) (<-chan types.Cmd, <-chan error) {
	cmd := make(chan types.Cmd)
	errc := make(chan error)

	go func() {
		defer close(cmd)
		defer close(errc)

		ts := time.Now().UTC().Unix()
		var lastTS int64
		var lastCmd defs.Command
		for {
			select {
			case <-cl:
				return
			default:
			}
			cur, err := m.CmdStreamCollection().Find(
				ctx,
				bson.M{"ts": bson.M{"$gte": ts}},
			)
			if err != nil {
				errc <- errors.Wrap(err, "watch the cmd stream")
				continue
			}

			for cur.Next(ctx) {
				c := types.Cmd{}
				err := cur.Decode(&c)
				if err != nil {
					errc <- errors.Wrap(err, "message decode")
					continue
				}

				if c.Cmd == lastCmd && c.TS == lastTS {
					continue
				}

				opid, ok := cur.Current.Lookup("_id").ObjectIDOK()
				if !ok {
					errc <- errors.New("unable to get operation ID")
					continue
				}

				c.OPID = types.OPID(opid)

				lastCmd = c.Cmd
				lastTS = c.TS
				cmd <- c
				ts = time.Now().UTC().Unix()
			}
			if err := cur.Err(); err != nil {
				errc <- cursorClosedError{err}
				cur.Close(ctx)
				return
			}
			cur.Close(ctx)
			time.Sleep(time.Second * 1)
		}
	}()

	return cmd, errc
}

type cursorClosedError struct {
	Err error
}

func (c cursorClosedError) Error() string {
	return "cursor was closed with:" + c.Err.Error()
}

func (c cursorClosedError) Is(err error) bool {
	if err == nil {
		return false
	}

	_, ok := err.(cursorClosedError) //nolint:errorlint
	return ok
}

func (c cursorClosedError) Unwrap() error {
	return c.Err
}

// Delete deletes backup(s) from the store and cleans up its metadata
func (a *Agent) Delete(ctx context.Context, d *types.DeleteBackupCmd, opid types.OPID, ep config.Epoch) {
	if d == nil {
		l := a.log.NewEvent(string(defs.CmdDeleteBackup), "", opid.String(), ep.TS())
		l.Error("missed command")
		return
	}

	l := a.pbm.Logger().NewEvent(string(defs.CmdDeleteBackup), "", opid.String(), ep.TS())
	ctx = log.SetLoggerToContext(ctx, a.log)

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.node.Session())
	if err != nil {
		l.Error("get node info data: %v", err)
		return
	}

	if !nodeInfo.IsLeader() {
		l.Info("not a member of the leader rs, skipping")
		return
	}

	epts := ep.TS()
	lock := lock.NewOpLock(a.pbm.Conn, lock.LockHeader{
		Replset: a.node.RS(),
		Node:    a.node.Name(),
		Type:    defs.CmdDeleteBackup,
		OPID:    opid.String(),
		Epoch:   &epts,
	})

	got, err := a.acquireLock(ctx, lock, l, nil)
	if err != nil {
		l.Error("acquire lock: %v", err)
		return
	}
	if !got {
		l.Debug("skip: lock not acquired")
		return
	}
	defer func() {
		if err := lock.Release(); err != nil {
			l.Error("release lock: %v", err)
		}
	}()

	switch {
	case d.OlderThan > 0:
		t := time.Unix(d.OlderThan, 0).UTC()
		obj := t.Format("2006-01-02T15:04:05Z")
		l = a.pbm.Logger().NewEvent(string(defs.CmdDeleteBackup), obj, opid.String(), ep.TS())
		l.Info("deleting backups older than %v", t)
		err := a.pbm.DeleteOlderThan(ctx, t, l)
		if err != nil {
			l.Error("deleting: %v", err)
			return
		}
	case d.Backup != "":
		l = a.pbm.Logger().NewEvent(string(defs.CmdDeleteBackup), d.Backup, opid.String(), ep.TS())
		l.Info("deleting backup")
		err := a.pbm.DeleteBackup(ctx, d.Backup, l)
		if err != nil {
			l.Error("deleting: %v", err)
			return
		}
	default:
		l.Error("malformed command received in Delete() of backup: %v", d)
		return
	}

	l.Info("done")
}

// DeletePITR deletes PITR chunks from the store and cleans up its metadata
func (a *Agent) DeletePITR(ctx context.Context, d *types.DeletePITRCmd, opid types.OPID, ep config.Epoch) {
	if d == nil {
		l := a.log.NewEvent(string(defs.CmdDeletePITR), "", opid.String(), ep.TS())
		l.Error("missed command")
		return
	}

	l := a.pbm.Logger().NewEvent(string(defs.CmdDeletePITR), "", opid.String(), ep.TS())
	ctx = log.SetLoggerToContext(ctx, a.log)

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.node.Session())
	if err != nil {
		l.Error("get node info data: %v", err)
		return
	}

	if !nodeInfo.IsLeader() {
		l.Info("not a member of the leader rs, skipping")
		return
	}

	epts := ep.TS()
	lock := lock.NewOpLock(a.pbm.Conn, lock.LockHeader{
		Replset: a.node.RS(),
		Node:    a.node.Name(),
		Type:    defs.CmdDeletePITR,
		OPID:    opid.String(),
		Epoch:   &epts,
	})

	got, err := a.acquireLock(ctx, lock, l, nil)
	if err != nil {
		l.Error("acquire lock: %v", err)
		return
	}
	if !got {
		l.Debug("skip: lock not acquired")
		return
	}
	defer func() {
		if err := lock.Release(); err != nil {
			l.Error("release lock: %v", err)
		}
	}()

	if d.OlderThan > 0 {
		t := time.Unix(d.OlderThan, 0).UTC()
		obj := t.Format("2006-01-02T15:04:05Z")
		l = a.pbm.Logger().NewEvent(string(defs.CmdDeletePITR), obj, opid.String(), ep.TS())
		l.Info("deleting pitr chunks older than %v", t)
		err = a.pbm.DeletePITR(ctx, &t, l)
	} else {
		l = a.pbm.Logger().NewEvent(string(defs.CmdDeletePITR), "_all_", opid.String(), ep.TS())
		l.Info("deleting all pitr chunks")
		err = a.pbm.DeletePITR(ctx, nil, l)
	}
	if err != nil {
		l.Error("deleting: %v", err)
		return
	}

	l.Info("done")
}

// Cleanup deletes backups and PITR chunks from the store and cleans up its metadata
func (a *Agent) Cleanup(ctx context.Context, d *types.CleanupCmd, opid types.OPID, ep config.Epoch) {
	l := a.log.NewEvent(string(defs.CmdCleanup), "", opid.String(), ep.TS())
	ctx = log.SetLoggerToContext(ctx, a.log)

	if d == nil {
		l.Error("missed command")
		return
	}

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.node.Session())
	if err != nil {
		l.Error("get node info data: %v", err)
		return
	}
	if !nodeInfo.IsLeader() {
		l.Info("not a member of the leader rs, skipping")
		return
	}

	epts := ep.TS()
	lock := lock.NewOpLock(a.pbm.Conn, lock.LockHeader{
		Replset: a.node.RS(),
		Node:    a.node.Name(),
		Type:    defs.CmdCleanup,
		OPID:    opid.String(),
		Epoch:   &epts,
	})

	got, err := a.acquireLock(ctx, lock, l, nil)
	if err != nil {
		l.Error("acquire lock: %v", err)
		return
	}
	if !got {
		l.Debug("skip: lock not acquired")
		return
	}
	defer func() {
		if err := lock.Release(); err != nil {
			l.Error("release lock: %v", err)
		}
	}()

	stg, err := util.GetStorage(ctx, a.pbm.Conn, l)
	if err != nil {
		l.Error("get storage: " + err.Error())
	}

	eg := errgroup.Group{}
	eg.SetLimit(runtime.NumCPU())

	cr, err := pbm.MakeCleanupInfo(ctx, a.pbm.Conn, d.OlderThan)
	if err != nil {
		l.Error("make cleanup report: " + err.Error())
		return
	}

	for i := range cr.Chunks {
		name := cr.Chunks[i].FName

		eg.Go(func() error {
			err := stg.Delete(name)
			return errors.Wrapf(err, "delete chunk file %q", name)
		})
	}
	if err := eg.Wait(); err != nil {
		l.Error(err.Error())
	}

	for i := range cr.Backups {
		bcp := &cr.Backups[i]

		eg.Go(func() error {
			err := a.pbm.DeleteBackupFiles(bcp, stg)
			return errors.Wrapf(err, "delete backup files %q", bcp.Name)
		})
	}
	if err := eg.Wait(); err != nil {
		l.Error(err.Error())
	}

	err = resync.ResyncStorage(ctx, a.pbm.Conn, l)
	if err != nil {
		l.Error("storage resync: " + err.Error())
	}
}

// Resync uploads a backup list from the remote store
func (a *Agent) Resync(ctx context.Context, opid types.OPID, ep config.Epoch) {
	l := a.pbm.Logger().NewEvent(string(defs.CmdResync), "", opid.String(), ep.TS())
	ctx = log.SetLoggerToContext(ctx, a.log)

	a.HbResume()
	a.pbm.Logger().ResumeMgo()

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.node.Session())
	if err != nil {
		l.Error("get node info data: %v", err)
		return
	}

	if !nodeInfo.IsLeader() {
		l.Info("not a member of the leader rs")
		return
	}

	epts := ep.TS()
	lock := lock.NewLock(a.pbm.Conn, lock.LockHeader{
		Type:    defs.CmdResync,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
		OPID:    opid.String(),
		Epoch:   &epts,
	})

	got, err := a.acquireLock(ctx, lock, l, nil)
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
			l.Error("reslase lock %v: %v", lock, err)
		}
	}()

	l.Info("started")
	err = resync.ResyncStorage(ctx, a.pbm.Conn, l)
	if err != nil {
		l.Error("%v", err)
		return
	}
	l.Info("succeed")

	epch, err := config.ResetEpoch(a.pbm.Conn)
	if err != nil {
		l.Error("reset epoch: %v", err)
		return
	}

	l.Debug("epoch set to %v", epch)
}

type lockAquireFn func(context.Context) (bool, error)

// acquireLock tries to acquire the lock. If there is a stale lock
// it tries to mark op that held the lock (backup, [pitr]restore) as failed.
func (a *Agent) acquireLock(ctx context.Context, l *lock.Lock, lg *log.Event, acquireFn lockAquireFn) (bool, error) {
	ctx = log.SetLoggerToContext(ctx, a.log)
	if acquireFn == nil {
		acquireFn = l.Acquire
	}

	got, err := acquireFn(ctx)
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
	case defs.CmdBackup:
		fn = lock.MarkBcpStale
	case defs.CmdRestore:
		fn = lock.MarkRestoreStale
	default:
		return acquireFn(ctx)
	}

	if err := fn(ctx, l, lck.OPID); err != nil {
		lg.Warning("failed to mark stale op '%s' as failed: %v", lck.OPID, err)
	}

	return acquireFn(ctx)
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
	l := a.log.NewEvent("agentCheckup", "", "", primitive.Timestamp{})
	ctx = log.SetLoggerToContext(ctx, a.log)

	nodeVersion, err := version.GetMongoVersion(ctx, a.node.Session())
	if err != nil {
		l.Error("get mongo version: %v", err)
	}

	hb := topo.AgentStat{
		Node:       a.node.Name(),
		RS:         a.node.RS(),
		AgentVer:   version.Current().Version,
		MongoVer:   nodeVersion.VersionString,
		PerconaVer: nodeVersion.PSMDBVersion,
	}
	defer func() {
		if err := topo.RemoveAgentStatus(ctx, a.pbm.Conn, hb); err != nil {
			logger := a.log.NewEvent("agentCheckup", "", "", primitive.Timestamp{})
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
		n, err := a.node.Status(ctx)
		if err != nil {
			l.Error("get replSetGetStatus: %v", err)
			hb.Err += fmt.Sprintf("get replSetGetStatus: %v", err)
		} else {
			hb.State = n.State
			hb.StateStr = n.StateStr
		}

		hb.Hidden = false
		hb.Passive = false

		inf, err := topo.GetNodeInfoExt(ctx, a.node.Session())
		if err != nil {
			l.Error("get NodeInfo: %v", err)
			hb.Err += fmt.Sprintf("get NodeInfo: %v", err)
		} else {
			hb.Hidden = inf.Hidden
			hb.Passive = inf.Passive
			hb.Arbiter = inf.ArbiterOnly
		}

		err = topo.SetAgentStatus(ctx, a.pbm.Conn, hb)
		if err != nil {
			l.Error("set status: %v", err)
		}
	}
}

func (a *Agent) pbmStatus(ctx context.Context) topo.SubsysStatus {
	err := a.pbm.Conn.MongoClient().Ping(ctx, nil)
	if err != nil {
		return topo.SubsysStatus{Err: err.Error()}
	}

	return topo.SubsysStatus{OK: true}
}

func (a *Agent) nodeStatus(ctx context.Context) topo.SubsysStatus {
	err := a.node.Session().Ping(ctx, nil)
	if err != nil {
		return topo.SubsysStatus{Err: err.Error()}
	}

	return topo.SubsysStatus{OK: true}
}

func (a *Agent) storStatus(ctx context.Context, log *log.Event, forceCheckStorage bool) topo.SubsysStatus {
	// check storage once in a while if all is ok (see https://jira.percona.com/browse/PBM-647)
	// but if storage was(is) failed, check it always
	stat, err := topo.GetAgentStatus(ctx, a.pbm.Conn, a.node.RS(), a.node.Name())
	if err != nil {
		log.Warning("get current storage status: %v", err)
	}
	if !forceCheckStorage && stat.StorageStatus.OK {
		return topo.SubsysStatus{OK: true}
	}

	stg, err := util.GetStorage(ctx, a.pbm.Conn, log)
	if err != nil {
		return topo.SubsysStatus{Err: fmt.Sprintf("unable to get storage: %v", err)}
	}

	_, err = stg.FileStat(defs.StorInitFile)
	if errors.Is(err, storage.ErrNotExist) {
		err := stg.Save(defs.StorInitFile, bytes.NewBufferString(version.Current().Version), 0)
		if err != nil {
			return topo.SubsysStatus{
				Err: fmt.Sprintf("storage: no init file, attempt to create failed: %v", err),
			}
		}
	} else if err != nil {
		return topo.SubsysStatus{Err: fmt.Sprintf("storage check failed with: %v", err)}
	}

	return topo.SubsysStatus{OK: true}
}

func logHbStatus(name string, st topo.SubsysStatus, l *log.Event) {
	if !st.OK {
		l.Error("check %s: %s", name, st.Err)
	}
}
