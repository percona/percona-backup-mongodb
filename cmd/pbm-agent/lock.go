package main

import (
	"context"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
)

func markBcpStale(ctx context.Context, conn connect.Client, opid string) error {
	bcp, err := backup.GetBackupByOPID(ctx, conn, opid)
	if err != nil {
		return errors.Wrap(err, "get backup meta")
	}

	// not to rewrite an error emitted by the agent
	if bcp.Status == defs.StatusError || bcp.Status == defs.StatusDone {
		return nil
	}

	log.Debug(ctx, "mark stale meta")
	return backup.ChangeBackupStateOPID(ctx, conn, opid, defs.StatusError,
		"some of pbm-agents were lost during the backup")
}

func markRestoreStale(ctx context.Context, conn connect.Client, opid string) error {
	r, err := restore.GetRestoreMetaByOPID(ctx, conn, opid)
	if err != nil {
		return errors.Wrap(err, "get retore meta")
	}

	// not to rewrite an error emitted by the agent
	if r.Status == defs.StatusError || r.Status == defs.StatusDone {
		return nil
	}

	log.Debug(ctx, "mark stale meta")
	return restore.ChangeRestoreStateOPID(ctx, conn, opid, defs.StatusError,
		"some of pbm-agents were lost during the restore")
}
