package agent

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/version"
)

type Agent struct {
	pbm     *pbm.PBM
	node    *pbm.Node
	bcp     *currentBackup
	pitrjob *currentPitr
	mx      sync.Mutex
	intent  uint32
	log     *log.Logger
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

func (a *Agent) AddNode(ctx context.Context, curi string) (err error) {
	a.node, err = pbm.NewNode(ctx, curi)
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
		ep, err := a.pbm.GetEpoch()
		if err != nil {
			a.log.Error("", "", "", ep.TS(), "get epoch: %v", err)
		}
		select {
		case cmd := <-c:
			a.log.Printf("got command %s", cmd)
			switch cmd.Cmd {
			case pbm.CmdBackup:
				// backup runs in the go-routine so it can be canceled
				go a.Backup(cmd.Backup, cmd.OPID, ep)
			case pbm.CmdCancelBackup:
				a.CancelBackup()
			case pbm.CmdRestore:
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

	lock := a.pbm.NewLockCol(pbm.LockHeader{
		Replset: a.node.RS(),
		Node:    a.node.Name(),
		Type:    pbm.CmdDeleteBackup,
		OPID:    opid.String(),
		Epoch:   ep.TS(),
	}, pbm.LockOpCollection)

	got, err := a.aquireLock(lock)
	if err != nil {
		l.Error("acquire lock: %v", err)
		return
	}
	if !got {
		l.Info("scheduled to another node")
		return
	}
	defer func() {
		err := lock.Release()
		if err != nil {
			l.Error("release lock: %v", err)
		}
	}()

	tsstart := time.Now()
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

	// TODO: timers logic should be replaced with the opID
	needToWait := waitAtLeast - time.Since(tsstart)
	if needToWait > 0 {
		time.Sleep(needToWait)
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

	lock := a.pbm.NewLock(pbm.LockHeader{
		Type:    pbm.CmdResyncBackupList,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
		OPID:    opid.String(),
		Epoch:   ep.TS(),
	})

	got, err := a.aquireLock(lock)
	if err != nil {
		l.Error("acquiring lock: %v", err)
		return
	}
	if !got {
		l.Info("operation has been scheduled on another replset node")
		return
	}

	tstart := time.Now()
	l.Info("started")
	err = a.pbm.ResyncStorage(l)
	if err != nil {
		l.Error("%v", err)
	} else {
		l.Info("succeed")
	}

	needToWait := time.Second*1 - time.Since(tstart)
	if needToWait > 0 {
		time.Sleep(needToWait)
	}
	err = lock.Release()
	if err != nil {
		l.Error("reslase lock %v: %v", lock, err)
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
	case pbm.ErrConcurrentOp:
		a.log.Printf("acquiring lock: %v", err)
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
			a.log.Warning("", "", "", l.Epoch, "failed to mark stale op '%s' as failed: %v", lk.OPID, merr)
		}
		return l.Acquire()
	default:
		return false, err
	}
}
