package main

import (
	"context"

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

func (a *Agent) handleApplyConfig(
	ctx context.Context,
	cmd *ctrl.ConfigCmd,
	opid ctrl.OPID,
	epoch config.Epoch,
) {
	if cmd == nil {
		log.Error(ctx, "missed command")
		return
	}

	var err error
	defer func() {
		if err != nil {
			log.Error(ctx, "failed to apply config: %v", err)
		}
	}()

	oldCfg, err := config.GetConfig(ctx, a.leadConn)
	if err != nil {
		log.Warn(ctx, "no config set")
		oldCfg = &config.Config{}
	}

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		err = errors.Wrap(err, "get node info")
		return
	}
	if !nodeInfo.IsClusterLeader() {
		log.Debug(ctx, "not the leader. skip")
		return
	}

	lck := lock.NewLock(a.leadConn, lock.LockHeader{
		Type:    ctrl.CmdApplyConfig,
		Replset: defs.Replset(),
		Node:    defs.NodeID(),
		OPID:    opid.String(),
		Epoch:   util.Ref(epoch.TS()),
	})

	got, err := a.acquireLock(ctx, lck)
	if err != nil {
		err = errors.Wrap(err, "acquiring lock")
		return
	}
	if !got {
		err = errors.New("lock not acquired")
		return
	}
	defer func() {
		log.Debug(ctx, "releasing lock")
		err := lck.Release()
		if err != nil {
			log.Error(ctx, "unable to release lock %v: %v", lck, err)
		}
	}()

	newCfg := &config.Config{
		Storage: *cmd.Storage,
		PITR:    cmd.PITR,
		Backup:  cmd.Backup,
		Restore: cmd.Restore,
		Logging: cmd.Logging,
	}
	err = config.SetConfig(ctx, a.leadConn, newCfg)
	if err != nil {
		err = errors.Wrap(err, "set config")
		return
	}

	log.Info(ctx, "config applied")

	if !oldCfg.Logging.Equal(cmd.Logging) {
		err1 := switchLogger(ctx, a.leadConn, cmd.Logging)
		if err1 != nil {
			err = errors.Wrap(err1, "failed to switch logger")
			return
		}
	}
}

func switchLogger(ctx context.Context, conn connect.Client, cfg *config.Logging) error {
	log.Warn(ctx, "changing local log handler")

	logHandler, err := log.NewFileHandler(cfg.Path, cfg.Level.Level(), cfg.JSON)
	if err != nil {
		return errors.Wrap(err, "create file handler")
	}

	prevLogHandler := log.SetLocalHandler(logHandler)

	log.Info(ctx, "pbm-agent:\n%s", version.Current().All(""))
	log.Info(ctx, "node: %s/%s", defs.Replset(), defs.NodeID())
	log.Info(ctx, "conn level ReadConcern: %v; WriteConcern: %v",
		conn.MongoOptions().ReadConcern.Level,
		conn.MongoOptions().WriteConcern.W)

	err = prevLogHandler.Close()
	if err != nil {
		log.Error(ctx, "close previous log handler: %v", err)
	}

	return nil
}

func (a *Agent) handleAddConfigProfile(
	ctx context.Context,
	cmd *ctrl.ProfileCmd,
	opid ctrl.OPID,
	epoch config.Epoch,
) {
	if cmd == nil {
		log.Error(ctx, "missed command")
		return
	}

	if cmd.Name == "" {
		log.Error(ctx, "missed config profile name")
		return
	}

	var err error
	defer func() {
		if err != nil {
			log.Error(ctx, "failed to add config profile: %v", err)
		}
	}()

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		err = errors.Wrap(err, "get node info")
		return
	}
	if !nodeInfo.IsClusterLeader() {
		log.Debug(ctx, "not the leader. skip")
		return
	}

	lck := lock.NewLock(a.leadConn, lock.LockHeader{
		Type:    ctrl.CmdAddConfigProfile,
		Replset: defs.Replset(),
		Node:    defs.NodeID(),
		OPID:    opid.String(),
		Epoch:   util.Ref(epoch.TS()),
	})

	got, err := a.acquireLock(ctx, lck)
	if err != nil {
		err = errors.Wrap(err, "acquiring lock")
		return
	}
	if !got {
		err = errors.New("lock not acquired")
		return
	}
	defer func() {
		log.Debug(ctx, "releasing lock")
		err := lck.Release()
		if err != nil {
			log.Error(ctx, "unable to release lock %v: %v", lck, err)
		}
	}()

	err = cmd.Storage.Cast()
	if err != nil {
		err = errors.Wrap(err, "storage cast")
		return
	}

	stg, err := util.StorageFromConfig(ctx, &cmd.Storage, defs.NodeID())
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

		err = util.Initialize(ctx, stg)
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

	log.Info(ctx, "profile saved")
}

func (a *Agent) handleRemoveConfigProfile(
	ctx context.Context,
	cmd *ctrl.ProfileCmd,
	opid ctrl.OPID,
	epoch config.Epoch,
) {
	if cmd == nil {
		log.Error(ctx, "missed command")
		return
	}

	if cmd.Name == "" {
		log.Error(ctx, "missed config profile name")
		return
	}

	var err error
	defer func() {
		if err != nil {
			log.Error(ctx, "failed to remove config profile: %v", err)
		}
	}()

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		err = errors.Wrap(err, "get node info")
		return
	}
	if !nodeInfo.IsClusterLeader() {
		log.Debug(ctx, "not the leader. skip")
		return
	}

	lck := lock.NewLock(a.leadConn, lock.LockHeader{
		Type:    ctrl.CmdRemoveConfigProfile,
		Replset: defs.Replset(),
		Node:    defs.NodeID(),
		OPID:    opid.String(),
		Epoch:   util.Ref(epoch.TS()),
	})

	got, err := a.acquireLock(ctx, lck)
	if err != nil {
		err = errors.Wrap(err, "acquiring lock")
		return
	}
	if !got {
		err = errors.New("lock not acquired")
		return
	}
	defer func() {
		log.Debug(ctx, "releasing lock")
		err := lck.Release()
		if err != nil {
			log.Error(ctx, "unable to release lock %v: %v", lck, err)
		}
	}()

	_, err = config.GetProfile(ctx, a.leadConn, cmd.Name)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			err = errors.Errorf("profile %q is not found", cmd.Name)
			return
		}

		err = errors.Wrap(err, "get config profile")
		return
	}

	err = resync.ClearBackupList(ctx, a.leadConn, cmd.Name)
	if err != nil {
		err = errors.Wrap(err, "clear backup list")
		return
	}

	err = config.RemoveProfile(ctx, a.leadConn, cmd.Name)
	if err != nil {
		err = errors.Wrap(err, "delete document")
		return
	}
}
