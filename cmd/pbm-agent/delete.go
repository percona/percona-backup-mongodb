package main

import (
	"context"
	"runtime"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/resync"
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

		l.Info("deleting backups older than %v", t)
		err = backup.DeleteBackupBefore(ctx, a.leadConn, t, bcpType)
		if err != nil {
			l.Error("deleting: %v", err)
			return
		}
	case d.Backup != "":
		l = logger.NewEvent(string(ctrl.CmdDeleteBackup), d.Backup, opid.String(), ep.TS())
		ctx := log.SetLogEventToContext(ctx, l)

		l.Info("deleting backup")
		err := backup.DeleteBackup(ctx, a.leadConn, d.Backup)
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

	ts := primitive.Timestamp{T: uint32(t.Unix())}
	l.Info("deleting pitr chunks older than %v", t)
	err = deletePITRImpl(ctx, a.leadConn, ts)
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

	stg, err := util.GetStorage(ctx, a.leadConn, l)
	if err != nil {
		l.Error("get storage: " + err.Error())
	}

	eg := errgroup.Group{}
	eg.SetLimit(runtime.NumCPU())

	cr, err := backup.MakeCleanupInfo(ctx, a.leadConn, d.OlderThan)
	if err != nil {
		l.Error("make cleanup report: " + err.Error())
		return
	}

	for i := range cr.Chunks {
		name := cr.Chunks[i].FName

		eg.Go(func() error {
			err := stg.Delete(name)
			return errors.Wrapf(err, "delete chunk file %q", name)
		})
	}
	if err := eg.Wait(); err != nil {
		l.Error(err.Error())
	}

	for i := range cr.Backups {
		bcp := &cr.Backups[i]

		eg.Go(func() error {
			err := backup.DeleteBackupFiles(bcp, stg)
			return errors.Wrapf(err, "delete backup files %q", bcp.Name)
		})
	}
	if err := eg.Wait(); err != nil {
		l.Error(err.Error())
	}

	err = resync.ResyncStorage(ctx, a.leadConn, l)
	if err != nil {
		l.Error("storage resync: " + err.Error())
	}
}

func deletePITRImpl(ctx context.Context, conn connect.Client, ts primitive.Timestamp) error {
	l := log.LogEventFromContext(ctx)

	cfg, err := config.GetConfig(ctx, conn)
	if err != nil {
		return errors.Wrap(err, "get config")
	}

	if cfg.PITR.Enabled && !cfg.PITR.OplogOnly {
		ct, err := topo.GetClusterTime(ctx, conn)
		if err != nil {
			return errors.Wrap(err, "get cluster time")
		}

		lw, err := backup.FindBaseSnapshotLWBefore(ctx, conn, ct, primitive.Timestamp{})
		if err != nil {
			return errors.Wrap(err, "find previous snapshot")
		}
		if !lw.IsZero() {
			if lw.T < ts.T || (lw.T == ts.T && (ts.I == 0 || lw.I < ts.I)) {
				ts = lw
			}
		}
	}

	chunks, err := oplog.ListDeleteChunksBefore(ctx, conn, ts)
	if err != nil {
		return errors.Wrap(err, "get pitr chunks")
	}
	if len(chunks) == 0 {
		l.Debug("nothing to delete")
		return nil
	}

	stg, err := util.StorageFromConfig(cfg.Storage, l)
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	return deleteChunks(ctx, conn, stg, chunks)
}

func deleteChunks(ctx context.Context, m connect.Client, stg storage.Storage, chunks []oplog.OplogChunk) error {
	l := log.LogEventFromContext(ctx)

	for _, chnk := range chunks {
		err := stg.Delete(chnk.FName)
		if err != nil && !errors.Is(err, storage.ErrNotExist) {
			return errors.Wrapf(err, "delete pitr chunk '%s' (%v) from storage", chnk.FName, chnk)
		}

		_, err = m.PITRChunksCollection().DeleteOne(
			ctx,
			bson.D{
				{"rs", chnk.RS},
				{"start_ts", chnk.StartTS},
				{"end_ts", chnk.EndTS},
			},
		)

		if err != nil {
			return errors.Wrap(err, "delete pitr chunk metadata")
		}

		l.Debug("deleted %s", chnk.FName)
	}

	return nil
}
