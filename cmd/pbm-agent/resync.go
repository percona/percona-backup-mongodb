package main

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/resync"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/util"
)

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
		err = handleSyncAllProfiles(ctx, a.leadConn, cmd.Clear)
	} else if cmd.Name != "" {
		err = handleSyncProfile(ctx, a.leadConn, cmd.Name, cmd.Clear)
	} else {
		err = handleSyncMainStorage(ctx, a.leadConn)
	}
	if err != nil {
		l.Error(err.Error())
		return
	}

	l.Info("succeed")
}

func handleSyncAllProfiles(ctx context.Context, conn connect.Client, clear bool) error {
	profiles, err := config.ListProfiles(ctx, conn)
	if err != nil {
		return errors.Wrap(err, "get config profiles")
	}

	eg, ctx := errgroup.WithContext(ctx)
	if clear {
		for i := range profiles {
			eg.Go(func() error {
				return helpClearProfileBackups(ctx, conn, profiles[i].Name)
			})
		}
	} else {
		for i := range profiles {
			eg.Go(func() error {
				return helpSyncProfileBackups(ctx, conn, &profiles[i])
			})
		}
	}

	return eg.Wait()
}

func handleSyncProfile(ctx context.Context, conn connect.Client, name string, clear bool) error {
	profile, err := config.GetProfile(ctx, conn, name)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			err = errors.Errorf("profile %q not found", name)
		}

		return errors.Wrap(err, "get config profile")
	}

	if clear {
		err = helpClearProfileBackups(ctx, conn, profile.Name)
	} else {
		err = helpSyncProfileBackups(ctx, conn, profile)
	}

	return err
}

func helpClearProfileBackups(ctx context.Context, conn connect.Client, profileName string) error {
	err := resync.ClearBackupList(ctx, conn, profileName)
	return errors.Wrapf(err, "clear backup list for %q", profileName)
}

func helpSyncProfileBackups(ctx context.Context, conn connect.Client, profile *config.Config) error {
	err := resync.SyncBackupList(ctx, conn, &profile.Storage, profile.Name)
	return errors.Wrapf(err, "sync backup list for %q", profile.Name)
}

func handleSyncMainStorage(ctx context.Context, conn connect.Client) error {
	cfg, err := config.GetConfig(ctx, conn)
	if err != nil {
		return errors.Wrap(err, "get config")
	}

	err = resync.Resync(ctx, conn, &cfg.Storage)
	if err != nil {
		return errors.Wrap(err, "resync")
	}

	epch, err := config.ResetEpoch(ctx, conn)
	if err != nil {
		return errors.Wrap(err, "reset epoch")
	}
	log.LogEventFromContext(ctx).
		Debug("epoch set to %v", epch)

	return nil
}
