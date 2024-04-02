package sdk

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
)

var (
	ErrUnsupported      = errors.New("unsupported")
	ErrInvalidCommandID = errors.New("invalid command id")
	ErrNotFound         = errors.New("not found")
)

type (
	CommandID string
	Timestamp = primitive.Timestamp
)

var NoOpID = CommandID(ctrl.NilOPID.String())

type BackupType = defs.BackupType

const (
	LogicalBackup     = defs.LogicalBackup
	PhysicalBackup    = defs.PhysicalBackup
	IncrementalBackup = defs.IncrementalBackup
	ExternalBackup    = defs.ExternalBackup
	SelectiveBackup   = backup.SelectiveBackup
)

type (
	CompressionType  = compress.CompressionType
	CompressionLevel *int
)

const (
	CompressionTypeNone      = compress.CompressionTypeNone
	CompressionTypeGZIP      = compress.CompressionTypeGZIP
	CompressionTypePGZIP     = compress.CompressionTypePGZIP
	CompressionTypeSNAPPY    = compress.CompressionTypeSNAPPY
	CompressionTypeLZ4       = compress.CompressionTypeLZ4
	CompressionTypeS2        = compress.CompressionTypeS2
	CompressionTypeZstandard = compress.CompressionTypeZstandard
)

type (
	Config          = config.Config
	BackupMetadata  = backup.BackupMeta
	RestoreMetadata = restore.RestoreMeta
	OplogChunk      = oplog.OplogChunk
	CleanupReport   = backup.CleanupInfo
)

type Lock interface {
	Type() string
	CommandID() string
}

type LogicalBackupOptions struct {
	Name             string
	Namespaces       []string
	CompressionType  CompressionType
	CompressionLevel CompressionLevel
}

type PhysicalBackupOptions struct {
	Name             string
	CompressionType  CompressionType
	CompressionLevel CompressionLevel
}

type IncrementalBackupOptions struct {
	Name             string
	NewBase          bool
	CompressionType  CompressionType
	CompressionLevel CompressionLevel
}

type ExternalBackupOptions struct {
	Name             string
	CompressionType  CompressionType
	CompressionLevel CompressionLevel
}

type GetBackupByNameOptions struct {
	FetchIncrements bool
	FetchFilelist   bool
}

type GetAllRestoresOptions struct {
	Limit int64
}

type DeleteBackupBeforeOptions struct {
	Type BackupType
}

type Command = ctrl.Cmd

type Client interface {
	Close(ctx context.Context) error

	ClusterMembers(ctx context.Context) ([]topo.Shard, error)

	CommandInfo(ctx context.Context, id CommandID) (*Command, error)

	GetConfig(ctx context.Context) (*Config, error)

	GetAllBackups(ctx context.Context) ([]BackupMetadata, error)
	GetBackupByName(ctx context.Context, name string, options GetBackupByNameOptions) (*BackupMetadata, error)

	GetAllRestores(ctx context.Context, m connect.Client, options GetAllRestoresOptions) ([]RestoreMetadata, error)
	GetRestoreByName(ctx context.Context, name string) (*RestoreMetadata, error)

	RunLogicalBackup(ctx context.Context, options LogicalBackupOptions) (CommandID, error)
	RunPhysicalBackup(ctx context.Context, options PhysicalBackupOptions) (CommandID, error)
	RunIncrementalBackup(ctx context.Context, options IncrementalBackupOptions) (CommandID, error)
	StartExternalBackup(ctx context.Context, options ExternalBackupOptions) (CommandID, error)
	CancelBackup(ctx context.Context) (CommandID, error)

	DeleteBackupByName(ctx context.Context, name string) (CommandID, error)
	DeleteBackupBefore(ctx context.Context, beforeTS Timestamp, options DeleteBackupBeforeOptions) (CommandID, error)

	DeleteOplogRange(ctx context.Context, until Timestamp) (CommandID, error)

	CleanupReport(ctx context.Context, beforeTS Timestamp) (CleanupReport, error)
	RunCleanup(ctx context.Context, beforeTS Timestamp) (CommandID, error)

	SyncFromStorage(ctx context.Context) (CommandID, error)
}

func NewClient(ctx context.Context, uri string) (Client, error) {
	conn, err := connect.Connect(ctx, uri, "sdk")
	if err != nil {
		return nil, err
	}

	return &clientImpl{conn: conn}, nil
}

func WaitForBackupStatus(
	ctx context.Context,
	client Client,
	cid CommandID,
	statuses ...defs.Status,
) (*BackupMetadata, error) {
	var bcp *BackupMetadata
	var err error

	db := backup.NewDBManager(client.(*clientImpl).conn)
	for range 5 {
		bcp, err = db.GetBackupByOpID(ctx, string(cid))
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				time.Sleep(time.Second)
				continue
			}

			return nil, errors.Wrap(err, "get backup")
		}
	}

	for {
		for _, s := range statuses {
			if bcp.Conditions.Has(s) {
				return bcp, nil
			}

			time.Sleep(5 * time.Second)

			bcp, err = db.GetBackupByOpID(ctx, string(cid))
			if err != nil {
				return nil, errors.Wrap(err, "wait for condition: get backup")
			}
		}
	}
}

func WaitForCleanup(ctx context.Context, client Client) error {
	lck := &lock.LockHeader{Type: ctrl.CmdCleanup}
	return waitOp(ctx, client.(*clientImpl).conn, lck)
}

func WaitForDeleteBackup(ctx context.Context, client Client) error {
	lck := &lock.LockHeader{Type: ctrl.CmdDeleteBackup}
	return waitOp(ctx, client.(*clientImpl).conn, lck)
}

func WaitForDeleteOplogRange(ctx context.Context, client Client) error {
	lck := &lock.LockHeader{Type: ctrl.CmdDeletePITR}
	return waitOp(ctx, client.(*clientImpl).conn, lck)
}

func WaitForErrorLog(ctx context.Context, client Client, cmd *Command) (string, error) {
	return lastLogErr(ctx, client.(*clientImpl).conn, cmd.Cmd, cmd.TS)
}
