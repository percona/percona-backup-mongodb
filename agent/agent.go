package agent

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
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
	log.Printf("node: %s", a.node.ID())

	c, cerr, err := a.pbm.ListenCmd()
	if err != nil {
		return err
	}

	for {
		select {
		case cmd := <-c:
			log.Printf("Got command %s [%v]", cmd.Cmd, cmd)
			switch cmd.Cmd {
			case pbm.CmdBackup:
				// backup runs in the go-routine so it can be canceled
				go a.Backup(cmd.Backup)
			case pbm.CmdCancelBackup:
				a.CancelBackup()
			case pbm.CmdRestore:
				a.Restore(cmd.Restore)
			case pbm.CmdResyncBackupList:
				a.ResyncBackupList()
			case pbm.CmdPITRestore:
				a.PITRestore(cmd.PITRestore)
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

// ResyncBackupList uploads a backup list from the remote store
func (a *Agent) ResyncBackupList() {
	nodeInfo, err := a.node.GetIsMaster()
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
	err = a.pbm.ResyncBackupList()
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
