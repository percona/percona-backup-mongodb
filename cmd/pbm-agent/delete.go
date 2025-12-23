package main

import (
	"context"
	"runtime"
	"time"

	bsonv2 "go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/util"
)

// Delete deletes backup(s) from the store and cleans up its metadata
func (a *Agent) Delete(ctx context.Context, d *ctrl.DeleteBackupCmd, opid ctrl.OPID, ep config.Epoch) {
	logger := log.FromContext(ctx)
	l := logger.NewEvent(string(ctrl.CmdDeleteBackup), "", opid.String(), ep.TS())

	if d == nil {
		l.Error("missed command")
		return
	}

	ctx = log.SetLogEventToContext(ctx, l)

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		l.Error("get node info data: %v", err)
		return
	}

	if !nodeInfo.IsLeader() {
		l.Info("not a member of the leader rs, skipping")
		return
	}

	epts := ep.TS()
	lock := lock.NewLock(a.leadConn, lock.LockHeader{
		Replset: a.brief.SetName,
		Node:    a.brief.Me,
		Type:    ctrl.CmdDeleteBackup,
		OPID:    opid.String(),
		Epoch:   &epts,
	})

	got, err := a.acquireLock(ctx, lock, l)
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

		l = logger.NewEvent(string(ctrl.CmdDeleteBackup), obj, opid.String(), ep.TS())
		ctx := log.SetLogEventToContext(ctx, l)

		ct, err := topo.GetClusterTime(ctx, a.leadConn)
		if err != nil {
			l.Error("get cluster time: %v", err)
			return
		}
		if d.OlderThan > int64(ct.T) {
			providedTime := t.Format(time.RFC3339)
			realTime := time.Unix(int64(ct.T), 0).UTC().Format(time.RFC3339)
			l.Error("provided time %q is after now %q", providedTime, realTime)
			return
		}

		bcpType, err := backup.ParseDeleteBackupType(string(d.Type))
		if err != nil {
			l.Error("parse type field: %v", err.Error())
			return
		}

		stg, err := util.GetProfiledStorage(ctx, a.leadConn, d.Profile, nodeInfo.Me, l)
		if err != nil {
			l.Error("get storage: %v", err)
			return
		}
		l.Info("deleting backups (profile: %q) older than %v", d.Profile, t)
		err = backup.DeleteBackupBefore(ctx, a.leadConn, stg, d.Profile, bcpType, t)
		if err != nil {
			l.Error("deleting: %v", err)
			return
		}
	case d.Backup != "":
		l.Info("deleting backup %q", d.Backup)
		err := backup.DeleteBackup(ctx, a.leadConn, d.Backup, nodeInfo.Me)
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
func (a *Agent) DeletePITR(ctx context.Context, d *ctrl.DeletePITRCmd, opid ctrl.OPID, ep config.Epoch) {
	logger := log.FromContext(ctx)
	l := logger.NewEvent(string(ctrl.CmdDeletePITR), "", opid.String(), ep.TS())

	if d == nil {
		l.Error("missed command")
		return
	}

	ctx = log.SetLogEventToContext(ctx, l)

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		l.Error("get node info data: %v", err)
		return
	}

	if !nodeInfo.IsLeader() {
		l.Info("not a member of the leader rs, skipping")
		return
	}

	epts := ep.TS()
	lock := lock.NewLock(a.leadConn, lock.LockHeader{
		Replset: a.brief.SetName,
		Node:    a.brief.Me,
		Type:    ctrl.CmdDeletePITR,
		OPID:    opid.String(),
		Epoch:   &epts,
	})

	got, err := a.acquireLock(ctx, lock, l)
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

	ct, err := topo.ClusterTimeFromNodeInfo(nodeInfo)
	if err != nil {
		l.Error("get cluster time: %v", err)
		return
	}

	t := time.Unix(d.OlderThan, 0).UTC()
	obj := t.Format("2006-01-02T15:04:05Z")

	l = logger.NewEvent(string(ctrl.CmdDeletePITR), obj, opid.String(), ep.TS())
	ctx = log.SetLogEventToContext(ctx, l)

	if d.OlderThan > int64(ct.T) {
		providedTime := t.Format(time.RFC3339)
		realTime := time.Unix(int64(ct.T), 0).UTC().Format(time.RFC3339)
		l.Error("provided time %q is after now %q", providedTime, realTime)
		return
	}

	ts := bsonv2.Timestamp{T: uint32(t.Unix())}
	l.Info("deleting pitr chunks older than %v", t)
	err = a.deletePITRImpl(ctx, ts)
	if err != nil {
		l.Error("deleting: %v", err)
		return
	}

	l.Info("done")
}

// Cleanup deletes backups and PITR chunks from the store and cleans up its metadata
func (a *Agent) Cleanup(ctx context.Context, d *ctrl.CleanupCmd, opid ctrl.OPID, ep config.Epoch) {
	logger := log.FromContext(ctx)
	l := logger.NewEvent(string(ctrl.CmdCleanup), "", opid.String(), ep.TS())

	if d == nil {
		l.Error("missed command")
		return
	}

	ctx = log.SetLogEventToContext(ctx, l)

	nodeInfo, err := topo.GetNodeInfoExt(ctx, a.nodeConn)
	if err != nil {
		l.Error("get node info data: %v", err)
		return
	}
	if !nodeInfo.IsLeader() {
		l.Info("not a member of the leader rs, skipping")
		return
	}

	epts := ep.TS()
	lock := lock.NewLock(a.leadConn, lock.LockHeader{
		Replset: a.brief.SetName,
		Node:    a.brief.Me,
		Type:    ctrl.CmdCleanup,
		OPID:    opid.String(),
		Epoch:   &epts,
	})

	got, err := a.acquireLock(ctx, lock, l)
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

	ct, err := topo.GetClusterTime(ctx, a.leadConn)
	if err != nil {
		l.Error("get cluster time: %v", err)
		return
	}
	if d.OlderThan.T > ct.T {
		providedTime := time.Unix(int64(ct.T), 0).UTC().Format(time.RFC3339)
		realTime := time.Unix(int64(ct.T), 0).UTC().Format(time.RFC3339)
		l.Error("provided time %q is after now %q", providedTime, realTime)
		return
	}

	cfg, err := config.GetProfiledConfig(ctx, a.leadConn, d.Profile)
	if err != nil {
		l.Error("get config: %v", err)
		return
	}

	stg, err := util.StorageFromConfig(&cfg.Storage, a.brief.Me, l)
	if err != nil {
		l.Error("get storage: " + err.Error())
		return
	}

	cr, err := backup.MakeCleanupInfo(ctx, a.leadConn, d.OlderThan, d.Profile)
	if err != nil {
		l.Error("make cleanup report: " + err.Error())
		return
	}

	eg := &errgroup.Group{}
	eg.SetLimit(runtime.NumCPU())

	if err := a.deleteChunks(ctx, eg, stg, cr.Chunks); err != nil {
		l.Error(err.Error())
	}

	if err := a.deleteBackups(ctx, eg, stg, cr.Backups); err != nil {
		l.Error(err.Error())
	}
}

func (a *Agent) deletePITRImpl(ctx context.Context, ts bsonv2.Timestamp) error {
	l := log.LogEventFromContext(ctx)

	r, err := backup.MakeCleanupInfo(ctx, a.leadConn, ts, "")
	if err != nil {
		return errors.Wrap(err, "get pitr chunks")
	}
	if len(r.Chunks) == 0 {
		l.Debug("nothing to delete")
		return nil
	}

	stg, err := util.GetStorage(ctx, a.leadConn, a.brief.Me, l)
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	eg := &errgroup.Group{}
	eg.SetLimit(runtime.NumCPU())
	return a.deleteChunks(ctx, eg, stg, r.Chunks)
}

func (a *Agent) deleteChunks(
	ctx context.Context,
	eg *errgroup.Group,
	stg storage.Storage,
	chunks []oplog.OplogChunk,
) error {
	for _, c := range chunks {
		eg.Go(func() error {
			err := oplog.DeleteChunkData(ctx, a.leadConn, stg, c)
			return errors.Wrapf(err, "delete chunk %q", c.FName)
		})
	}
	return eg.Wait()
}

func (a *Agent) deleteBackups(
	ctx context.Context,
	eg *errgroup.Group,
	stg storage.Storage,
	backups []backup.BackupMeta,
) error {
	l := log.LogEventFromContext(ctx)

	for _, b := range backups {
		eg.Go(func() error {
			l.Info("deleting backup %q (profile: %q)", b.Name, b.Store.Name)
			err := backup.DeleteBackupData(ctx, a.leadConn, stg, b.Name)
			return errors.Wrapf(err, "delete backup %q", b.Name)
		})
	}
	return eg.Wait()
}
