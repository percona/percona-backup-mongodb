package sdk

import (
	"context"
	"path"
	"runtime"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/util"
	"github.com/percona/percona-backup-mongodb/pbm/version"
)

var ErrNotImplemented = errors.New("not implemented")

var (
	ErrBackupInProgress     = backup.ErrBackupInProgress
	ErrIncrementalBackup    = backup.ErrIncrementalBackup
	ErrNonIncrementalBackup = backup.ErrNonIncrementalBackup
	ErrNotBaseIncrement     = backup.ErrNotBaseIncrement
	ErrBaseForPITR          = backup.ErrBaseForPITR
)

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

func (c *clientImpl) GetAllConfigProfiles(ctx context.Context) ([]config.Config, error) {
	return config.ListProfiles(ctx, c.conn)
}

func (c *clientImpl) GetConfigProfile(ctx context.Context, name string) (*config.Config, error) {
	profile, err := config.GetProfile(ctx, c.conn, name)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			err = config.ErrMissedConfigProfile
		}
		return nil, err
	}

	return profile, nil
}

func (c *clientImpl) AddConfigProfile(ctx context.Context, name string, cfg *Config) (CommandID, error) {
	opid, err := ctrl.SendAddConfigProfile(ctx, c.conn, name, cfg.Storage)
	return CommandID(opid.String()), err
}

func (c *clientImpl) RemoveConfigProfile(ctx context.Context, name string) (CommandID, error) {
	opid, err := ctrl.SendRemoveConfigProfile(ctx, c.conn, name)
	return CommandID(opid.String()), err
}

func (c *clientImpl) GetAllBackups(ctx context.Context) ([]BackupMetadata, error) {
	return backup.BackupsList(ctx, c.conn, 0)
}

func (c *clientImpl) GetAllRestores(
	ctx context.Context,
	m connect.Client,
	options GetAllRestoresOptions,
) ([]RestoreMetadata, error) {
	limit := options.Limit
	if limit < 0 {
		limit = 0
	}
	return restore.RestoreList(ctx, c.conn, limit)
}

func (c *clientImpl) GetBackupByName(
	ctx context.Context,
	name string,
	options GetBackupByNameOptions,
) (*BackupMetadata, error) {
	bcp, err := backup.NewDBManager(c.conn).GetBackupByName(ctx, name)
	if err != nil {
		return nil, err
	}

	if options.FetchIncrements && bcp.Type == IncrementalBackup {
		if bcp.SrcBackup != "" {
			return nil, ErrNotBaseIncrement
		}

		increments, err := backup.FetchAllIncrements(ctx, c.conn, bcp)
		if err != nil {
			return nil, errors.New("get increments")
		}
		if increments == nil {
			// use non-nil empty slice to mark fetch.
			// nil means it never tried to fetch before
			increments = make([][]*backup.BackupMeta, 0)
		}

		bcp.Increments = increments
	}

	if options.FetchFilelist {
		err = fillFilelistForBackup(ctx, bcp)
		if err != nil {
			return nil, errors.Wrap(err, "fetch filelist")
		}
	}

	return bcp, nil
}

func fillFilelistForBackup(ctx context.Context, bcp *BackupMetadata) error {
	var err error
	var stg storage.Storage

	eg, _ := errgroup.WithContext(ctx)
	eg.SetLimit(runtime.NumCPU())

	if version.HasFilelistFile(bcp.PBMVersion) {
		stg, err = util.StorageFromConfig(&bcp.Store.StorageConf, log.LogEventFromContext(ctx))
		if err != nil {
			return errors.Wrap(err, "get storage")
		}

		for i := range bcp.Replsets {
			rs := &bcp.Replsets[i]

			eg.Go(func() error {
				filelist, err := getFilelistForReplset(stg, bcp.Name, rs.Name)
				if err != nil {
					return errors.Wrapf(err, "get filelist for %q [rs: %s] backup", bcp.Name, rs.Name)
				}

				rs.Files = filelist
				return nil
			})
		}
	}

	for i := range bcp.Increments {
		for j := range bcp.Increments[i] {
			bcp := bcp.Increments[i][j]

			if bcp.Status != defs.StatusDone {
				continue
			}
			if !version.HasFilelistFile(bcp.PBMVersion) {
				continue
			}

			if stg == nil {
				// in case if it is the first backup made with filelist file
				stg, err = getStorageForRead(ctx, bcp)
				if err != nil {
					return errors.Wrap(err, "get storage")
				}
			}

			for i := range bcp.Replsets {
				rs := &bcp.Replsets[i]

				eg.Go(func() error {
					filelist, err := getFilelistForReplset(stg, bcp.Name, rs.Name)
					if err != nil {
						return errors.Wrapf(err, "fetch files for %q [rs: %s] backup", bcp.Name, rs.Name)
					}

					rs.Files = filelist
					return nil
				})
			}
		}
	}

	return eg.Wait()
}

func getStorageForRead(ctx context.Context, bcp *backup.BackupMeta) (storage.Storage, error) {
	stg, err := util.StorageFromConfig(&bcp.Store.StorageConf, log.LogEventFromContext(ctx))
	if err != nil {
		return nil, errors.Wrap(err, "get storage")
	}
	err = storage.HasReadAccess(ctx, stg)
	if err != nil && !errors.Is(err, storage.ErrUninitialized) {
		return nil, errors.Wrap(err, "check storage access")
	}

	return stg, nil
}

func getFilelistForReplset(stg storage.Storage, bcpName, rsName string) (backup.Filelist, error) {
	pfFilepath := path.Join(bcpName, rsName, backup.FilelistName)
	rdr, err := stg.SourceReader(pfFilepath)
	if err != nil {
		return nil, errors.Wrapf(err, "open %q", pfFilepath)
	}
	defer rdr.Close()

	filelist, err := backup.ReadFilelist(rdr)
	if err != nil {
		return nil, errors.Wrapf(err, "parse filelist %q", pfFilepath)
	}

	return filelist, nil
}

func (c *clientImpl) GetRestoreByName(ctx context.Context, name string) (*RestoreMetadata, error) {
	return restore.GetRestoreMeta(ctx, c.conn, name)
}

func (c *clientImpl) SyncFromStorage(ctx context.Context) (CommandID, error) {
	opid, err := ctrl.SendResync(ctx, c.conn)
	return CommandID(opid.String()), err
}

func (c *clientImpl) SyncFromExternalStorage(ctx context.Context, name string) (CommandID, error) {
	if name == "" {
		return NoOpID, errors.New("name is not provided")
	}

	opid, err := ctrl.SendSyncMetaFrom(ctx, c.conn, name)
	return CommandID(opid.String()), err
}

func (c *clientImpl) SyncFromAllExternalStorages(ctx context.Context) (CommandID, error) {
	opid, err := ctrl.SendSyncMetaFrom(ctx, c.conn, "")
	return CommandID(opid.String()), err
}

func (c *clientImpl) ClearSyncFromExternalStorage(ctx context.Context, name string) (CommandID, error) {
	if name == "" {
		return NoOpID, errors.New("name is not provided")
	}

	opid, err := ctrl.SendClearMetaFrom(ctx, c.conn, name)
	return CommandID(opid.String()), err
}

func (c *clientImpl) ClearSyncFromAllExternalStorages(ctx context.Context) (CommandID, error) {
	opid, err := ctrl.SendClearMetaFrom(ctx, c.conn, "")
	return CommandID(opid.String()), err
}

func (c *clientImpl) DeleteBackupByName(ctx context.Context, name string) (CommandID, error) {
	opts := GetBackupByNameOptions{FetchIncrements: true}
	bcp, err := c.GetBackupByName(ctx, name, opts)
	if err != nil {
		return NoOpID, errors.Wrap(err, "get backup meta")
	}
	if bcp.Type == defs.IncrementalBackup {
		err = CanDeleteIncrementalBackup(ctx, c, bcp, bcp.Increments)
	} else {
		err = CanDeleteBackup(ctx, c, bcp)
	}
	if err != nil {
		return NoOpID, err
	}

	opid, err := ctrl.SendDeleteBackupByName(ctx, c.conn, name)
	return CommandID(opid.String()), err
}

func (c *clientImpl) DeleteBackupBefore(
	ctx context.Context,
	beforeTS Timestamp,
	options DeleteBackupBeforeOptions,
) (CommandID, error) {
	opid, err := ctrl.SendDeleteBackupBefore(ctx, c.conn, beforeTS, options.Type)
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

			if clusterTime.T-lock.Heartbeat.T >= defs.StaleFrameSec {
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
		TimeMin: time.Unix(after, 0),
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
