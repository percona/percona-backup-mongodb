package sdk

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/internal/backup"
	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/ctrl"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/lock"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/restore"
	"github.com/percona/percona-backup-mongodb/internal/topo"
)

var ErrNotImplemented = errors.New("not implemented")

type clientImpl struct {
	conn connect.Client
}

func (c *clientImpl) Close(ctx context.Context) error {
	return c.conn.Disconnect(ctx)
}

func (c *clientImpl) ClusterMembers(ctx context.Context) ([]topo.Shard, error) {
	return topo.ClusterMembers(ctx, c.conn.MongoClient())
}

func (c *clientImpl) CommandInfo(ctx context.Context, id CommandID) (*Command, error) {
	opid, err := ctrl.OPIDfromStr(string(id))
	if err != nil {
		return nil, ErrInvalidCommandID
	}

	res := c.conn.CmdStreamCollection().FindOne(ctx, bson.D{{"_id", opid.Obj()}})
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, errors.Wrap(err, "query")
	}

	cmd := &Command{}
	if err = res.Decode(&cmd); err != nil {
		return nil, errors.Wrap(err, "decode")
	}

	cmd.OPID = opid
	return cmd, nil
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

func (c *clientImpl) GetAllRestores(ctx context.Context, m connect.Client) ([]RestoreMetadata, error) {
	return restore.RestoreList(ctx, c.conn, 0)
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

func (c *clientImpl) DeleteBackupByName(ctx context.Context, name string) (CommandID, error) {
	meta, err := backup.NewDBManager(c.conn).GetBackupByName(ctx, name)
	if err != nil {
		return NoOpID, errors.Wrap(err, "get backup meta")
	}

	err = backup.CanDeleteBackup(ctx, c.conn, meta)
	if err != nil {
		return NoOpID, err
	}

	opid, err := ctrl.SendDeleteBackupByName(ctx, c.conn, name)
	return CommandID(opid.String()), err
}

func (c *clientImpl) DeleteBackupBefore(ctx context.Context, beforeTS Timestamp) (CommandID, error) {
	opid, err := ctrl.SendDeleteBackupBefore(ctx, c.conn, beforeTS, "")
	return CommandID(opid.String()), err
}

func (c *clientImpl) DeleteOplogRange(ctx context.Context, until Timestamp) (CommandID, error) {
	opid, err := ctrl.SendDeleteOplogRangeBefore(ctx, c.conn, until)
	return CommandID(opid.String()), err
}

func (c *clientImpl) CleanupReport(ctx context.Context, beforeTS Timestamp) (CleanupReport, error) {
	return backup.MakeCleanupInfo(ctx, c.conn, beforeTS)
}

func (c *clientImpl) RunCleanup(ctx context.Context, beforeTS Timestamp) (CommandID, error) {
	opid, err := ctrl.SendCleanup(ctx, c.conn, beforeTS)
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

func WaitForBackupDelete(ctx context.Context, client Client) error {
	lck := &lock.LockHeader{Type: ctrl.CmdDeleteBackup}
	return waitOp(ctx, client.(*clientImpl).conn, lck)
}

func WaitForErrorLog(ctx context.Context, client Client, commandID CommandID) (string, error) {
	cmd, err := client.CommandInfo(ctx, commandID)
	if err != nil {
		return "", errors.Wrap(err, "get command info")
	}

	return lastLogErr(ctx, client.(*clientImpl).conn, cmd.Cmd, cmd.TS)
}

// waitOp waits until operations which acquires a given lock are finished
func waitOp(ctx context.Context, conn connect.Client, lck *lock.LockHeader) error {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			lock, err := lock.GetLockData(ctx, conn, lck)
			if err != nil {
				if errors.Is(err, mongo.ErrNoDocuments) {
					// No lock, so operation has finished
					return nil
				}

				return errors.Wrap(err, "get lock data")
			}

			clusterTime, err := topo.GetClusterTime(ctx, conn)
			if err != nil {
				return errors.Wrap(err, "read cluster time")
			}

			if clusterTime.T-lock.Heartbeat.T < defs.StaleFrameSec {
				return errors.Errorf("operation stale, last beat ts: %d", lock.Heartbeat.T)
			}
		}
	}
}

func lastLogErr(ctx context.Context, cc connect.Client, op ctrl.Command, after int64) (string, error) {
	r := &log.LogRequest{
		LogKeys: log.LogKeys{
			Severity: log.Error,
			Event:    string(op),
		},
	}

	outC, errC := log.Follow(ctx, cc, r, false)

	for {
		select {
		case entry := <-outC:
			return entry.Msg, nil
		case err := <-errC:
			return "", err
		}
	}
}
