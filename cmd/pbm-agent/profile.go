package main

import (
	"context"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/util"
)

func (a *Agent) handleAddConfigProfile(
	ctx context.Context,
	cmd *ctrl.ProfileCmd,
	opid ctrl.OPID,
	epoch config.Epoch,
) {
	logger := log.FromContext(ctx)

	if cmd == nil {
		l := logger.NewEvent(string(ctrl.CmdAddConfigProfile), "", opid.String(), epoch.TS())
		l.Error("missed command")
		return
	}
	if cmd.Name == "" {
		l := logger.NewEvent(string(ctrl.CmdAddConfigProfile), "", opid.String(), epoch.TS())
		l.Error("missed config profile name")
		return
	}

	l := logger.NewEvent(string(ctrl.CmdAddConfigProfile), cmd.Name, opid.String(), epoch.TS())
	ctx = log.SetLogEventToContext(ctx, l)

	var err error
	defer func() {
		if err != nil {
			l.Error("failed to add config profile: %v", err)
		}
	}()

	nodeInfo, err := topo.GetNodeInfo(ctx, a.nodeConn)
	if err != nil {
		err = errors.Wrap(err, "get node info")
		return
	}
	if !nodeInfo.IsClusterLeader() {
		l.Debug("not leader. skip")
		return
	}

	lck := lock.NewLock(a.leadConn, lock.LockHeader{
		Type:    ctrl.CmdAddConfigProfile,
		Replset: a.brief.SetName,
		Node:    a.brief.Me,
		OPID:    opid.String(),
		Epoch:   util.Ref(epoch.TS()),
	})

	got, err := a.acquireLock(ctx, lck, l)
	if err != nil {
		l.Error("acquiring lock: %v", err)
		return
	}
	if !got {
		l.Error("lock not acquired")
		return
	}
	defer func() {
		l.Debug("releasing lock")
		err = lck.Release()
		if err != nil {
			l.Error("unable to release lock %v: %v", lck, err)
		}
	}()

	err = cmd.Storage.Cast()
	if err != nil {
		l.Error("storage cast: %v", err)
		return
	}

	stg, err := util.StorageFromConfig(&cmd.Storage, log.LogEventFromContext(ctx))
	if err != nil {
		err = errors.Wrap(err, "storage from config")
		return
	}

	err = storage.HasReadAccess(ctx, stg)
	if err != nil {
		if !errors.Is(err, storage.ErrUninitialized) {
			err = errors.Wrap(err, "check read access")
			return
		}

		err = storage.InitStorage(ctx, stg)
		if err != nil {
			err = errors.Wrap(err, "init storage")
			return
		}
	}

	profile := &config.Config{
		Name:      cmd.Name,
		IsProfile: true,
		Storage:   cmd.Storage,
	}
	err = config.AddProfile(ctx, a.leadConn, profile)
	if err != nil {
		err = errors.Wrap(err, "add profile config")
		return
	}
}

func (a *Agent) handleRemoveConfigProfile(
	ctx context.Context,
	cmd *ctrl.ProfileCmd,
	opid ctrl.OPID,
	epoch config.Epoch,
) {
	logger := log.FromContext(ctx)

	if cmd == nil {
		l := logger.NewEvent(string(ctrl.CmdRemoveConfigProfile), "", opid.String(), epoch.TS())
		l.Error("missed command")
		return
	}
	if cmd.Name == "" {
		l := logger.NewEvent(string(ctrl.CmdRemoveConfigProfile), "", opid.String(), epoch.TS())
		l.Error("missed config profile name")
		return
	}

	l := logger.NewEvent(string(ctrl.CmdRemoveConfigProfile), cmd.Name, opid.String(), epoch.TS())
	ctx = log.SetLogEventToContext(ctx, l)

	var err error
	defer func() {
		if err != nil {
			l.Error("failed to remove config profile: %v", err)
		}
	}()

	nodeInfo, err := topo.GetNodeInfo(ctx, a.nodeConn)
	if err != nil {
		err = errors.Wrap(err, "get node info")
		return
	}
	if !nodeInfo.IsClusterLeader() {
		l.Debug("not leader. skip")
		return
	}

	lck := lock.NewLock(a.leadConn, lock.LockHeader{
		Type:    ctrl.CmdRemoveConfigProfile,
		Replset: a.brief.SetName,
		Node:    a.brief.Me,
		OPID:    opid.String(),
		Epoch:   util.Ref(epoch.TS()),
	})

	got, err := a.acquireLock(ctx, lck, l)
	if err != nil {
		l.Error("acquiring lock: %v", err)
		return
	}
	if !got {
		l.Error("lock not acquired")
		return
	}
	defer func() {
		l.Debug("releasing lock")
		err = lck.Release()
		if err != nil {
			l.Error("unable to release lock %v: %v", lck, err)
		}
	}()

	err = config.RemoveProfile(ctx, a.leadConn, cmd.Name)
	if err != nil {
		l.Error("delete document", err)
		return
	}
}
