package sdk

import (
	"context"
	"errors"

	"github.com/percona/percona-backup-mongodb/internal/backup"
	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/ctrl"
	"github.com/percona/percona-backup-mongodb/internal/lock"
	"github.com/percona/percona-backup-mongodb/internal/restore"
)

var ErrNotImplemented = errors.New("not implemented")

type resultImpl struct {
	opid CommandID
	err  error
}

func (r resultImpl) ID() CommandID {
	return r.opid
}

func (r resultImpl) Err() error {
	return r.err
}

type clientImpl struct {
	conn connect.Client
}

func (c *clientImpl) Close(ctx context.Context) error {
	return c.conn.Disconnect(ctx)
}

func (c *clientImpl) CurrentLocks(ctx context.Context) ([]Lock, error) {
	return nil, ErrNotImplemented
}

func (c *clientImpl) GetConfig(ctx context.Context) (*Config, error) {
	return config.GetConfig(ctx, c.conn)
}

func (c *clientImpl) SetConfig(ctx context.Context, cfg Config) (CommandID, error) {
	return NoOpID, config.SetConfig(ctx, c.conn, cfg)
}

func (c *clientImpl) GetAllBackups(ctx context.Context) ([]BackupMetadata, error) {
	return backup.BackupsList(ctx, c.conn, 0)
}

func (c *clientImpl) GetBackupByName(ctx context.Context, name string) (*BackupMetadata, error) {
	return backup.NewDBManager(c.conn).GetBackupByName(ctx, name)
}

func (c *clientImpl) GetRestoreByName(ctx context.Context, name string) (*RestoreMetadata, error) {
	return restore.GetRestoreMeta(ctx, c.conn, name)
}

func (c *clientImpl) SyncFromStorage(ctx context.Context) (CommandID, error) {
	opid, err := ctrl.SendResync(ctx, c.conn)
	return CommandID(opid.String()), err
}

func (c *clientImpl) CancelBackup(ctx context.Context) (CommandID, error) {
	opid, err := ctrl.SendCancelBackup(ctx, c.conn)
	return CommandID(opid.String()), err
}

func (c *clientImpl) RunLogicalBackup(ctx context.Context, options LogicalBackupOptions) (CommandID, error) {
	return NoOpID, ErrNotImplemented
}

func (c *clientImpl) RunPhysicalBackup(ctx context.Context, options PhysicalBackupOptions) (CommandID, error) {
	return NoOpID, ErrNotImplemented
}

func (c *clientImpl) RunIncrementalBackup(ctx context.Context, options IncrementalBackupOptions) (CommandID, error) {
	return NoOpID, ErrNotImplemented
}

func (c *clientImpl) Restore(ctx context.Context, backupName string, clusterTS Timestamp) (CommandID, error) {
	return NoOpID, ErrNotImplemented
}

type lockImpl struct {
	lock.LockData
}

func (l lockImpl) Type() string {
	return string(l.LockData.Type)
}

func (l lockImpl) CommandID() string {
	return l.OPID
}

func (l lockImpl) Heartbeat() Timestamp {
	return l.LockData.Heartbeat
}

func (c *clientImpl) CurrentOperations(ctx context.Context) ([]Lock, error) {
	rv := []Lock{}

	locks, err := lock.GetLocks(ctx, c.conn, &lock.LockHeader{})
	if err != nil {
		return nil, err
	}
	for _, l := range locks {
		rv = append(rv, lockImpl{l})
	}

	locks, err = lock.GetOpLocks(ctx, c.conn, &lock.LockHeader{})
	if err != nil {
		return nil, err
	}
	for _, l := range locks {
		rv = append(rv, lockImpl{l})
	}

	return rv, nil
}

func wrapResult(opid ctrl.OPID, err error) resultImpl {
	return resultImpl{opid: CommandID(opid.String()), err: err}
}
