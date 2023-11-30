package main

import (
	"context"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/internal/backup"
	"github.com/percona/percona-backup-mongodb/internal/ctrl"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/lock"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/restore"
)

func markBcpStale(ctx context.Context, l *lock.Lock, opid string) error {
	bcp, err := backup.GetBackupByOPID(ctx, l.Connect(), opid)
	if err != nil {
		return errors.Wrap(err, "get backup meta")
	}

	// not to rewrite an error emitted by the agent
	if bcp.Status == defs.StatusError || bcp.Status == defs.StatusDone {
		return nil
	}

	log.FromContext(ctx).Debug(string(ctrl.CmdBackup), "", opid, primitive.Timestamp{}, "mark stale meta")

	return backup.ChangeBackupStateOPID(l.Connect(), opid, defs.StatusError,
		"some of pbm-agents were lost during the backup")
}

func markRestoreStale(ctx context.Context, l *lock.Lock, opid string) error {
	r, err := restore.GetRestoreMetaByOPID(ctx, l.Connect(), opid)
	if err != nil {
		return errors.Wrap(err, "get retore meta")
	}

	// not to rewrite an error emitted by the agent
	if r.Status == defs.StatusError || r.Status == defs.StatusDone {
		return nil
	}

	log.FromContext(ctx).Debug(string(ctrl.CmdRestore), "", opid, primitive.Timestamp{}, "mark stale meta")

	return restore.ChangeRestoreStateOPID(ctx, l.Connect(), opid, defs.StatusError,
		"some of pbm-agents were lost during the restore")
}
