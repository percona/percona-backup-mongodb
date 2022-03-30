package agent

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/version"
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

func New(pbm *pbm.PBM) *Agent {
	return &Agent{
		pbm:      pbm,
		closeCMD: make(chan struct{}),
	}
}

func (a *Agent) AddNode(ctx context.Context, curi string, dumpConns int) (err error) {
	a.node, err = pbm.NewNode(ctx, curi, dumpConns)
	return err
}

func (a *Agent) InitLogger(cn *pbm.PBM) {
	a.pbm.InitLogger(a.node.RS(), a.node.Name())
	a.log = a.pbm.Logger()
}

// Start starts listening the commands stream.
func (a *Agent) Start() error {
	a.log.Printf("pbm-agent:\n%s", version.DefaultInfo.All(""))
	a.log.Printf("node: %s", a.node.ID())

	c, cerr := a.pbm.ListenCmd(a.closeCMD)

	a.log.Printf("listening for the commands")

	for {
		select {
		case cmd, ok := <-c:
			if !ok {
				a.log.Printf("change stream was closed")
				return nil
			}

			a.log.Printf("got command %s", cmd)

			ep, err := a.pbm.GetEpoch()
			if err != nil {
				a.log.Error(string(cmd.Cmd), "", cmd.OPID.String(), ep.TS(), "get epoch: %v", err)
				continue
			}

			a.log.Printf("got epoch %v", ep)

			switch cmd.Cmd {
			case pbm.CmdBackup:
				// backup runs in the go-routine so it can be canceled
				go a.Backup(cmd.Backup, cmd.OPID, ep)
			case pbm.CmdCancelBackup:
				a.CancelBackup()
			case pbm.CmdRestore:
				a.Restore(cmd.Restore, cmd.OPID, ep)
			case pbm.CmdReplay:
				a.OplogReplay(cmd.Replay, cmd.OPID, ep)
			case pbm.CmdResync:
				a.Resync(cmd.OPID, ep)
			case pbm.CmdPITRestore:
				a.PITRestore(cmd.PITRestore, cmd.OPID, ep)
			case pbm.CmdDeleteBackup:
				a.Delete(cmd.Delete, cmd.OPID, ep)
			case pbm.CmdDeletePITR:
				a.DeletePITR(cmd.DeletePITR, cmd.OPID, ep)
			}
		case err, ok := <-cerr:
			if !ok {
				a.log.Printf("change stream was closed")
				return nil
			}

			switch err.(type) {
			case pbm.ErrorCursor:
				return errors.Wrap(err, "stop listening")
			default:
				ep, _ := a.pbm.GetEpoch()

				a.log.Error("", "", "", ep.TS(), "listening commands: %v", err)
			}
		}
	}
}

// Delete deletes backup(s) from the store and cleans up its metadata
func (a *Agent) Delete(d pbm.DeleteBackupCmd, opid pbm.OPID, ep pbm.Epoch) {
	l := a.pbm.Logger().NewEvent(string(pbm.CmdDeleteBackup), "", opid.String(), ep.TS())

	nodeInfo, err := a.node.GetInfo()
	if err != nil {
		l.Error("get node info data: %v", err)
		return
	}

	if !nodeInfo.IsLeader() {
		l.Info("not a member of the leader rs, skipping")
		return
	}

	epts := ep.TS()
	lock := a.pbm.NewLockCol(pbm.LockHeader{
		Replset: a.node.RS(),
		Node:    a.node.Name(),
		Type:    pbm.CmdDeleteBackup,
		OPID:    opid.String(),
		Epoch:   &epts,
	}, pbm.LockOpCollection)

	got, err := a.acquireLock(lock, l, nil)
	if err != nil {
		l.Error("acquire lock: %v", err)
		return
	}
	if !got {
		l.Debug("skip: lock not acquired")
		return
	}
	defer func() {
		err := lock.Release()
		if err != nil {
			l.Error("release lock: %v", err)
		}
	}()

	switch {
	case d.OlderThan > 0:
		t := time.Unix(d.OlderThan, 0).UTC()
		obj := t.Format("2006-01-02T15:04:05Z")
		l = a.pbm.Logger().NewEvent(string(pbm.CmdDeleteBackup), obj, opid.String(), ep.TS())
		l.Info("deleting backups older than %v", t)
		err := a.pbm.DeleteOlderThan(t, l)
		if err != nil {
			l.Error("deleting: %v", err)
			return
		}
	case d.Backup != "":
		l = a.pbm.Logger().NewEvent(string(pbm.CmdDeleteBackup), d.Backup, opid.String(), ep.TS())
		l.Info("deleting backup")
		err := a.pbm.DeleteBackup(d.Backup, l)
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
func (a *Agent) DeletePITR(d pbm.DeletePITRCmd, opid pbm.OPID, ep pbm.Epoch) {
	l := a.pbm.Logger().NewEvent(string(pbm.CmdDeletePITR), "", opid.String(), ep.TS())

	nodeInfo, err := a.node.GetInfo()
	if err != nil {
		l.Error("get node info data: %v", err)
		return
	}

	if !nodeInfo.IsLeader() {
		l.Info("not a member of the leader rs, skipping")
		return
	}

	epts := ep.TS()
	lock := a.pbm.NewLockCol(pbm.LockHeader{
		Replset: a.node.RS(),
		Node:    a.node.Name(),
		Type:    pbm.CmdDeletePITR,
		OPID:    opid.String(),
		Epoch:   &epts,
	}, pbm.LockOpCollection)

	got, err := a.acquireLock(lock, l, nil)
	if err != nil {
		l.Error("acquire lock: %v", err)
		return
	}
	if !got {
		l.Debug("skip: lock not acquired")
		return
	}
	defer func() {
		err := lock.Release()
		if err != nil {
			l.Error("release lock: %v", err)
		}
	}()

	if d.OlderThan > 0 {
		t := time.Unix(d.OlderThan, 0).UTC()
		obj := t.Format("2006-01-02T15:04:05Z")
		l = a.pbm.Logger().NewEvent(string(pbm.CmdDeletePITR), obj, opid.String(), ep.TS())
		l.Info("deleting pitr chunks older than %v", t)
		err = a.pbm.DeletePITR(&t, l)
	} else {
		l = a.pbm.Logger().NewEvent(string(pbm.CmdDeletePITR), "_all_", opid.String(), ep.TS())
		l.Info("deleting all pitr chunks")
		err = a.pbm.DeletePITR(nil, l)
	}
	if err != nil {
		l.Error("deleting: %v", err)
		return
	}

	l.Info("done")
}

// Resync uploads a backup list from the remote store
func (a *Agent) Resync(opid pbm.OPID, ep pbm.Epoch) {
	l := a.pbm.Logger().NewEvent(string(pbm.CmdResync), "", opid.String(), ep.TS())

	a.HbResume()
	a.pbm.Logger().ResumeMgo()

	nodeInfo, err := a.node.GetInfo()
	if err != nil {
		l.Error("get node info data: %v", err)
		return
	}

	if !nodeInfo.IsLeader() {
		l.Info("not a member of the leader rs")
		return
	}

	epts := ep.TS()
	lock := a.pbm.NewLock(pbm.LockHeader{
		Type:    pbm.CmdResync,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
		OPID:    opid.String(),
		Epoch:   &epts,
	})

	got, err := a.acquireLock(lock, l, nil)
	if err != nil {
		l.Error("acquiring lock: %v", err)
		return
	}
	if !got {
		l.Debug("lock not acquired")
		return
	}

	defer func() {
		err = lock.Release()
		if err != nil {
			l.Error("reslase lock %v: %v", lock, err)
		}
	}()

	l.Info("started")
	err = a.pbm.ResyncStorage(l)
	if err != nil {
		l.Error("%v", err)
		return
	}
	l.Info("succeed")

	epch, err := a.pbm.ResetEpoch()
	if err != nil {
		l.Error("reset epoch: %v", err)
		return
	}

	l.Debug("epoch set to %v", epch)
}

type lockAquireFn func() (bool, error)

// acquireLock tries to acquire the lock. If there is a stale lock
// it tries to mark op that held the lock (backup, [pitr]restore) as failed.
func (a *Agent) acquireLock(l *pbm.Lock, lg *log.Event, acquireFn lockAquireFn) (got bool, err error) {
	if acquireFn == nil {
		acquireFn = l.Acquire
	}

	got, err = acquireFn()
	if err == nil {
		return got, nil
	}

	switch err := err.(type) {
	case pbm.ErrDuplicateOp, pbm.ErrConcurrentOp:
		lg.Debug("get lock: %v", err)
		return false, nil
	case pbm.ErrWasStaleLock:
		lk := err.Lock
		lg.Debug("stale lock: %v", lk)
		var fn func(opid string) error
		switch lk.Type {
		case pbm.CmdBackup:
			fn = a.pbm.MarkBcpStale
		case pbm.CmdRestore, pbm.CmdPITRestore:
			fn = a.pbm.MarkRestoreStale
		default:
			return acquireFn()
		}
		merr := fn(lk.OPID)
		if merr != nil {
			lg.Warning("failed to mark stale op '%s' as failed: %v", lk.OPID, merr)
		}
		return acquireFn()
	default:
		return false, err
	}
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

func (a *Agent) HbStatus() {
	hb := pbm.AgentStat{
		Node: a.node.Name(),
		RS:   a.node.RS(),
		Ver:  version.DefaultInfo.Version,
	}
	defer func() {
		if err := a.pbm.RmAgentStatus(hb); err != nil {
			logger := a.log.NewEvent("agentCheckup", "", "", primitive.Timestamp{})
			logger.Error("remove agent heartbeat: %v", err)
		}
	}()

	tk := time.NewTicker(pbm.AgentsStatCheckRange)
	defer tk.Stop()

	l := a.log.NewEvent("agentCheckup", "", "", primitive.Timestamp{})

	// check storage once in a while if all is ok (see https://jira.percona.com/browse/PBM-647)
	const checkStoreIn = int(60 / (pbm.AgentsStatCheckRange / time.Second))
	var cc int

	for range tk.C {
		// don't check if on pause (e.g. physical restore)
		if !a.HbIsRun() {
			continue
		}

		hb.PBMStatus = a.pbmStatus()
		logHbStatus("PBM connection", hb.PBMStatus, l)

		hb.NodeStatus = a.nodeStatus()
		logHbStatus("node connection", hb.NodeStatus, l)

		cc++
		hb.StorageStatus = a.storStatus(l, cc == checkStoreIn)
		logHbStatus("storage connection", hb.StorageStatus, l)
		if cc == checkStoreIn {
			cc = 0
		}

		hb.Err = ""

		hb.State = pbm.NodeStateUnknown
		hb.StateStr = "unknown"
		n, err := a.node.Status()
		if err != nil {
			l.Error("get replSetGetStatus: %v", err)
			hb.Err += fmt.Sprintf("get replSetGetStatus: %v", err)
		} else {
			hb.State = n.State
			hb.StateStr = n.StateStr
		}

		hb.Hidden = false
		hb.Passive = false

		inf, err := a.node.GetInfo()
		if err != nil {
			l.Error("get NodeInfo: %v", err)
			hb.Err += fmt.Sprintf("get NodeInfo: %v", err)
		} else {
			hb.Hidden = inf.Hidden
			hb.Passive = inf.Passive
		}

		err = a.pbm.SetAgentStatus(hb)
		if err != nil {
			l.Error("set status: %v", err)
		}
	}
}

func (a *Agent) pbmStatus() (sts pbm.SubsysStatus) {
	err := a.pbm.Conn.Ping(a.pbm.Context(), nil)
	if err != nil {
		sts.OK = false
		sts.Err = err.Error()
		return
	}

	sts.OK = true
	return
}

func (a *Agent) nodeStatus() (sts pbm.SubsysStatus) {
	err := a.node.Session().Ping(a.pbm.Context(), nil)
	if err != nil {
		sts.OK = false
		sts.Err = err.Error()
		return
	}

	sts.OK = true
	return
}

func (a *Agent) storStatus(log *log.Event, forceCheckStorage bool) (sts pbm.SubsysStatus) {
	sts.OK = false

	// check storage once in a while if all is ok (see https://jira.percona.com/browse/PBM-647)
	// but if storage was(is) failed, check it always
	stat, err := a.pbm.GetAgentStatus(a.node.RS(), a.node.Name())
	if err != nil {
		log.Warning("get current storage status: %v", err)
	}
	if !forceCheckStorage && stat.StorageStatus.OK {

		sts.OK = true
		return sts
	}

	stg, err := a.pbm.GetStorage(log)
	if err != nil {
		sts.Err = fmt.Sprintf("unable to get storage: %v", err)
		return sts
	}

	_, err = stg.FileStat(pbm.StorInitFile)
	if errors.Is(err, storage.ErrNotExist) {
		err := stg.Save(pbm.StorInitFile, bytes.NewBufferString(version.DefaultInfo.Version), 0)
		if err != nil {
			sts.Err = fmt.Sprintf("storage: no init file, attempt to create failed: %v", err)
			return sts
		}
	} else if err != nil {
		sts.Err = fmt.Sprintf("storage check failed with: %v", err)
		return sts
	}

	sts.OK = true
	return sts
}

func logHbStatus(name string, st pbm.SubsysStatus, l *log.Event) {
	if !st.OK {
		l.Error("check %s: %s", name, st.Err)
	}
}
