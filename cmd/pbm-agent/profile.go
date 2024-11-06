package main

import (
	"context"
	"time"

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

type applyConfigState string

const (
	applyConfigInit     applyConfigState = "init"
	applyConfigVerified applyConfigState = "verified"
	applyConfigSaved    applyConfigState = "saved"
	applyConfigApplied  applyConfigState = "applied"
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
			log.Error(ctx, err.Error())
		}
	}()

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		err = errors.Wrap(err, "get node info")
		return
	}

	// only the leader acquires lock and sets config.
	// others report about config suitability for their processes
	leader := nodeInfo.IsClusterLeader()

	// acquire lock on cluster leader to prevent other command.
	// but run the command on all agents to verify config suitability for each agent.
	// TODO: add the flow to handleAddConfigProfile
	if leader {
		lck := lock.NewLock(a.leadConn, lock.LockHeader{
			Type:    ctrl.CmdApplyConfig,
			Replset: defs.Replset(),
			Node:    defs.NodeID(),
			OPID:    opid.String(),
			Epoch:   util.Ref(epoch.TS()),
		})

		var got bool
		got, err = a.acquireLock(ctx, lck)
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

		// set status on leader.
		// backup and restore have state machine using their metadata.
		// other commands/operations rely on pbm logs for results.
		// TODO: PBM-950
		log.Info(ctx, string(applyConfigInit))
	}

	// wait for status from single node - the leader.
	// TODO: rewrite with PBM-950
	err = waitForState(ctx, a.leadConn, opid, 1, applyConfigInit)
	if err != nil {
		err = errors.Wrapf(err, "wait for status %s", applyConfigInit)
		return
	}

	oldCfg, err := config.GetConfig(ctx, a.leadConn)
	if err != nil {
		log.Warn(ctx, "no config set")
		oldCfg = &config.Config{}
	}

	newCfg := &config.Config{
		Storage: *cmd.Storage,
		PITR:    cmd.PITR,
		Backup:  cmd.Backup,
		Restore: cmd.Restore,
		Logging: cmd.Logging,
		Epoch:   oldCfg.Epoch,
	}

	numAgents, err := countAgents(ctx, a.leadConn)
	if err != nil {
		err = errors.Wrap(err, "count agents")
		return
	}
	if numAgents < 1 {
		err = errors.Errorf("unexpected number of agents: %v", numAgents)
		return
	}

	if !oldCfg.Storage.Equal(&newCfg.Storage) {
		// check storage availability from the agent.
		var stg storage.Storage
		stg, err = util.StorageFromConfig(ctx, &newCfg.Storage, defs.NodeID())
		if err != nil {
			err = errors.Wrap(err, "get storage")
			return
		}

		err = storage.HasReadAccess(ctx, stg)
		if err != nil {
			if !errors.Is(err, storage.ErrUninitialized) {
				err = errors.Wrap(err, "check read access")
				return
			}

			if leader {
				err = util.Initialize(ctx, stg)
				if err != nil {
					err = errors.Wrap(err, "init storage")
					return
				}
			}
		}
	}

	var logHandler *log.FileHandler
	if !oldCfg.Logging.Equal(newCfg.Logging) {
		// check log config on the agent.
		logHandler, err = log.NewFileHandler(
			newCfg.Logging.Path,
			newCfg.Logging.Level.Level(),
			newCfg.Logging.JSON)
		if err != nil {
			err = errors.Wrap(err, "create file handler")
			return
		}

		defer func() {
			if err != nil {
				// if config is not applied, revert (just close file)
				err1 := logHandler.Close()
				if err1 != nil {
					log.Error(ctx, "failed to close unused log handler")
				}
			}
		}()
	}

	// TODO: rewrite with PBM-950
	// set status on each node.
	log.Debug(ctx, string(applyConfigVerified))
	// wait for the status or error from all agents.
	err = waitForState(ctx, a.leadConn, opid, numAgents, applyConfigVerified)
	if err != nil {
		err = errors.Wrapf(err, "wait for status %s", applyConfigVerified)
		return
	}

	if leader {
		// save config by leader
		err = config.SetConfig(ctx, a.leadConn, newCfg)
		if err != nil {
			err = errors.Wrap(err, "save config")
			return
		}

		log.Debug(ctx, string(applyConfigSaved))
	}

	// almost done. if config saving on leader fails, config is not applied anywhere.
	// needs to wait before applying new log handler
	err = waitForState(ctx, a.leadConn, opid, 1, applyConfigSaved)
	if err != nil {
		err = errors.Wrapf(err, "wait for status %s", applyConfigSaved)
		return
	}

	if logHandler != nil {
		// there was changes in log config. apply by each agent.
		log.Warn(ctx, "changing local log handler")

		prevLogHandler := log.SetLocalHandler(logHandler)

		log.Info(ctx, "pbm-agent:\n%s", version.Current().All(""))
		log.Info(ctx, "node: %s/%s", defs.Replset(), defs.NodeID())
		log.Info(ctx, "conn level ReadConcern: %v; WriteConcern: %v",
			a.leadConn.MongoOptions().ReadConcern.Level,
			a.leadConn.MongoOptions().WriteConcern.W)

		// if closing handler fails, config is still suitable
		// and new logger are ready for use. print error but proceed.
		err := prevLogHandler.Close()
		if err != nil {
			log.Error(ctx, "close previous log handler: %v", err)
		}
	}

	log.Info(ctx, string(applyConfigApplied))
}

func countAgents(ctx context.Context, conn connect.Client) (int, error) {
	agents, err := topo.ListAgentStatuses(ctx, conn)
	if err != nil {
		return 0, err
	}
	return len(agents), nil
}

func waitForState(
	ctx context.Context,
	conn connect.Client,
	opid ctrl.OPID,
	count int,
	state applyConfigState,
) error {
	// TODO: expose timeout control
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// TODO: consider performance impact of tail cursors from all agents
	outC, errC := log.Follow(ctx, conn, &log.LogRequest{
		RecordAttrs: log.RecordAttrs{
			OPID:  opid.String(),
			Level: log.DebugLevel,
		},
	}, false)

	for {
		select {
		case entry := <-outC:
			if entry.Msg == string(state) {
				count -= 1 // found
				if count == 0 {
					return nil // all found
				}
			} else if entry.Level == log.ErrorLevel {
				return errors.Errorf("%s failed: %s", entry.Node, entry.Msg)
			}
		case err := <-errC:
			return err
		case <-ctx.Done():
			err := ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				err = errors.New("no response confirmation from agents")
			}
			return err
		}
	}
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
