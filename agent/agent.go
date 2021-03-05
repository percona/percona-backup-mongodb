package agent

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/version"
)

type Agent struct {
	pbm      *pbm.PBM
	node     *pbm.Node
	bcp      *currentBackup
	pitrjob  *currentPitr
	pitrsync chan struct{} // to update pitr state on demand (not to wait for the tick)
	mx       sync.Mutex
	intent   uint32
	log      *log.Logger
}

const (
	intentNone uint32 = iota
	intentBackup
)

func New(pbm *pbm.PBM) *Agent {
	return &Agent{
		pbm: pbm,
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

	c, cerr, err := a.pbm.ListenCmd()
	if err != nil {
		return err
	}

	a.log.Printf("listening for the commands")

	for {
		select {
		case cmd := <-c:
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
				a.ResyncPITR() // TODO: resync pitr on config update?
				a.Restore(cmd.Restore, cmd.OPID, ep)
			case pbm.CmdResyncBackupList:
				a.ResyncStorage(cmd.OPID, ep)
			case pbm.CmdPITRestore:
				a.PITRestore(cmd.PITRestore, cmd.OPID, ep)
			case pbm.CmdDeleteBackup:
				a.Delete(cmd.Delete, cmd.OPID, ep)
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

				ep, _ := a.pbm.GetEpoch()

				a.log.Error("", "", "", ep.TS(), "listening commands: %v", err)
			}
		}
	}
}

// Delete deletes backup(s) from the store and cleans up its metadata
func (a *Agent) Delete(d pbm.DeleteBackupCmd, opid pbm.OPID, ep pbm.Epoch) {
	const waitAtLeast = time.Second * 5
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

	got, err := a.aquireLock(lock)
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

// ResyncStorage uploads a backup list from the remote store
func (a *Agent) ResyncStorage(opid pbm.OPID, ep pbm.Epoch) {
	l := a.pbm.Logger().NewEvent(string(pbm.CmdResyncBackupList), "", opid.String(), ep.TS())

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
		Type:    pbm.CmdResyncBackupList,
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

	if nodeInfo.IsLeader() {
		epch, err := a.pbm.ResetEpoch()
		if err != nil {
			l.Error("reset epoch: %v", err)
			return
		}

		l.Debug("epoch set to %v", epch)
	}
}

// aquireLock tries to aquire the lock. If there is a stale lock
// it tries to mark op that held the lock (backup, [pitr]restore) as failed.
func (a *Agent) aquireLock(l *pbm.Lock) (got bool, err error) {
	got, err = l.Acquire()
	if err == nil {
		return got, nil
	}

	switch err.(type) {
	case pbm.ErrDuplicateOp, pbm.ErrConcurrentOp:
		a.log.Debug("", "", l.OPID, *l.Epoch, "get lock: %v", err)
		return false, nil
	case pbm.ErrWasStaleLock:
		lk := err.(pbm.ErrWasStaleLock).Lock
		var fn func(opid string) error
		switch lk.Type {
		case pbm.CmdBackup:
			fn = a.pbm.MarkBcpStale
		case pbm.CmdRestore, pbm.CmdPITRestore:
			fn = a.pbm.MarkRestoreStale
		default:
			return l.Acquire()
		}
		merr := fn(lk.OPID)
		if merr != nil {
			a.log.Warning("", "", "", *l.Epoch, "failed to mark stale op '%s' as failed: %v", lk.OPID, merr)
		}
		return l.Acquire()
	default:
		return false, err
	}
}

func (a *Agent) HbStatus() {
	hb := pbm.AgentStat{
		Node: a.node.Name(),
		RS:   a.node.RS(),
		Ver:  version.DefaultInfo.Version,
	}
	defer a.pbm.RmAgentStatus(hb)

	tk := time.NewTicker(pbm.AgentsStatCheckRange)
	defer tk.Stop()

	l := a.log.NewEvent("agentCheckup", "", "", primitive.Timestamp{})

	for range tk.C {
		hb.PBMStatus = a.pbmStatus()
		logHbStatus("PBM connecion", hb.PBMStatus, l)

		hb.NodeStatus = a.nodeStatus()
		logHbStatus("node connecion", hb.NodeStatus, l)

		hb.StorageStatus = a.storStatus(l)
		logHbStatus("storage connecion", hb.StorageStatus, l)

		err := a.pbm.SetAgentStatus(hb)
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

func (a *Agent) storStatus(log *log.Event) (sts pbm.SubsysStatus) {
	sts.OK = false

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
