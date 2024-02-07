package main

import (
	"context"
	"runtime"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/internal/backup"
	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/ctrl"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/lock"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/oplog/oplogtmp"
	"github.com/percona/percona-backup-mongodb/internal/resync"
	"github.com/percona/percona-backup-mongodb/internal/topo"
	"github.com/percona/percona-backup-mongodb/internal/util"
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

		l.Info("deleting backups older than %v", t)
		err = backup.DeleteBackupBefore(ctx, a.leadConn, t, "")
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

	if d.OlderThan > 0 {
		t := time.Unix(d.OlderThan, 0).UTC()
		obj := t.Format("2006-01-02T15:04:05Z")

		l = logger.NewEvent(string(ctrl.CmdDeletePITR), obj, opid.String(), ep.TS())
		ctx := log.SetLogEventToContext(ctx, l)

		var ct primitive.Timestamp
		ct, err = topo.GetClusterTime(ctx, a.leadConn)
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

		l.Info("deleting pitr chunks older than %v", t)
		err = oplogtmp.DeletePITR(ctx, a.leadConn, &t, l)
	} else {
		l = logger.NewEvent(string(ctrl.CmdDeletePITR), "_all_", opid.String(), ep.TS())
		ctx := log.SetLogEventToContext(ctx, l)
		l.Info("deleting all pitr chunks")
		err = oplogtmp.DeletePITR(ctx, a.leadConn, nil, l)
	}
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
