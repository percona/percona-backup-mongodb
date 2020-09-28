package agent

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/version"
)

type Agent struct {
	pbm     *pbm.PBM
	node    *pbm.Node
	bcp     *currentBackup
	pitrjob *currentPitr
	mx      sync.Mutex
	intent  uint32
	log     *pbm.Logger
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
	a.node.InitLogger(cn)
	a.log = a.node.Log
}

// Start starts listening the commands stream.
func (a *Agent) Start() error {
	a.log.Printf("pbm-agent:\n\t%s", version.DefaultInfo.All(""))
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
			switch cmd.Cmd {
			case pbm.CmdBackup:
				// backup runs in the go-routine so it can be canceled
				go a.Backup(cmd.Backup)
			case pbm.CmdCancelBackup:
				a.CancelBackup()
			case pbm.CmdRestore:
				a.Restore(cmd.Restore)
			case pbm.CmdResyncBackupList:
				a.ResyncStorage()
			case pbm.CmdPITRestore:
				a.PITRestore(cmd.PITRestore)
			case pbm.CmdDeleteBackup:
				a.Delete(cmd.Delete)
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

				a.log.Error(pbm.CmdUndefined, "", "listening commands: %v", err)
			}
		}
	}
}

// Delete deletes backup(s) from the store and cleans up its metadata
func (a *Agent) Delete(d pbm.DeleteBackupCmd) {
	nodeInfo, err := a.node.GetInfo()
	if err != nil {
		a.log.Error(pbm.CmdDeleteBackup, "", "get node info data: %v", err)
		return
	}

	if !nodeInfo.IsLeader() {
		a.log.Info(pbm.CmdDeleteBackup, "", "not a member of the leader rs, skipping")
		return
	}

	lock := a.pbm.NewLockCol(pbm.LockHeader{
		Replset: a.node.RS(),
		Node:    a.node.Name(),
		Type:    pbm.CmdDeleteBackup,
	}, pbm.LockOpCollection)

	got, err := a.aquireLock(lock, nil)
	if err != nil {
		a.log.Error(pbm.CmdDeleteBackup, "", "acquire lock: %v", err)
		return
	}
	if !got {
		a.log.Info(pbm.CmdDeleteBackup, "", "scheduled to another node")
		return
	}
	defer func() {
		err := lock.Release()
		if err != nil {
			a.log.Error(pbm.CmdDeleteBackup, "", "release lock: %v", err)
		}
	}()

	switch {
	case d.OlderThan > 0:
		t := time.Unix(d.OlderThan, 0).UTC()
		tstr := t.Format("2006-01-02T15:04:05Z")
		a.log.Info(pbm.CmdDeleteBackup, tstr, "deleting backups older than %v", t)
		err := a.pbm.DeleteOlderThan(t)
		if err != nil {
			a.log.Error(pbm.CmdDeleteBackup, tstr, "deleting: %v", err)
			return
		}
		a.log.Info(pbm.CmdDeleteBackup, tstr, "done")
	case d.Backup != "":
		a.log.Info(pbm.CmdDeleteBackup, d.Backup, "deleting backup")
		err := a.pbm.DeleteBackup(d.Backup)
		if err != nil {
			a.log.Error(pbm.CmdDeleteBackup, d.Backup, "deleting: %v", err)

			return
		}
		a.log.Info(pbm.CmdDeleteBackup, d.Backup, "done")
	default:
		a.log.Error(pbm.CmdDeleteBackup, "", "malformed command received in Delete() of backup: %v", d)

	}
}

// ResyncStorage uploads a backup list from the remote store
func (a *Agent) ResyncStorage() {
	nodeInfo, err := a.node.GetInfo()
	if err != nil {
		a.log.Error(pbm.CmdResyncBackupList, "", "get node info data: %v", err)
		return
	}

	if !nodeInfo.IsLeader() {
		a.log.Info(pbm.CmdResyncBackupList, "", "not a member of the leader rs")
		return
	}

	lock := a.pbm.NewLock(pbm.LockHeader{
		Type:    pbm.CmdResyncBackupList,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
	})

	got, err := lock.Acquire()
	if err != nil {
		switch err.(type) {
		case pbm.ErrConcurrentOp:
			a.log.Info(pbm.CmdResyncBackupList, "", "acquiring lock: %v", err)
		default:
			a.log.Error(pbm.CmdResyncBackupList, "", "acquiring lock: %v", err)
		}
		return
	}
	if !got {
		a.log.Info(pbm.CmdResyncBackupList, "", "operation has been scheduled on another replset node")
		return
	}

	tstart := time.Now()
	a.log.Info(pbm.CmdResyncBackupList, "", "started")
	err = a.pbm.ResyncStorage()
	if err != nil {
		a.log.Error(pbm.CmdResyncBackupList, "", "%v", err)
	} else {
		a.log.Info(pbm.CmdResyncBackupList, "", "succeed")
	}

	needToWait := time.Second*1 - time.Since(tstart)
	if needToWait > 0 {
		time.Sleep(needToWait)
	}
	err = lock.Release()
	if err != nil {
		a.log.Error(pbm.CmdResyncBackupList, "", "reslase lock %v: %v", lock, err)
	}
}

func (a *Agent) aquireLock(l *pbm.Lock, m func(name string) error) (got bool, err error) {
	got, err = l.Acquire()
	if err == nil {
		return got, nil
	}

	switch err.(type) {
	case pbm.ErrConcurrentOp:
		a.log.Info(pbm.CmdUndefined, "", "acquiring lock: %v", err)
		return false, nil
	case pbm.ErrWasStaleLock:
		if m != nil {
			name := err.(pbm.ErrWasStaleLock).Lock.BackupName
			merr := m(name)
			if merr != nil {
				a.log.Warning(pbm.CmdUndefined, "", "failed to mark stale backup '%s' as failed: %v", name, merr)
			}
		}
		return l.Acquire()
	default:
		return false, err
	}
}
