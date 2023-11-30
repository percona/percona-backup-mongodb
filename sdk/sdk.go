package sdk

import (
	"context"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/internal/backup"
	"github.com/percona/percona-backup-mongodb/internal/compress"
	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/ctrl"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/lock"
	"github.com/percona/percona-backup-mongodb/internal/restore"
	"github.com/percona/percona-backup-mongodb/internal/topo"
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

var NoOpID = CommandID(ctrl.NilOPID().String())

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
	CleanupReport   = backup.CleanupInfo
)

type Lock interface {
	Type() string
	CommandID() string
}

type LogicalBackupOptions struct {
	CompressionType  CompressionType
	CompressionLevel CompressionLevel
	Namespaces       []string
}

type PhysicalBackupOptions struct {
	CompressionType  CompressionType
	CompressionLevel CompressionLevel
}

type IncrementalBackupOptions struct {
	NewBase          bool
	CompressionType  CompressionType
	CompressionLevel CompressionLevel
}

type GetBackupByNameOptions struct {
	FetchIncrements bool
}

type GetAllRestoresOptions struct {
	Limit int64
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

	CancelBackup(ctx context.Context) (CommandID, error)

	DeleteBackupByName(ctx context.Context, name string) (CommandID, error)
	DeleteBackupBefore(ctx context.Context, beforeTS Timestamp) (CommandID, error)

	DeleteOplogRange(ctx context.Context, until Timestamp) (CommandID, error)

	CleanupReport(ctx context.Context, beforeTS Timestamp) (CleanupReport, error)
	RunCleanup(ctx context.Context, beforeTS Timestamp) (CommandID, error)

	SyncFromStorage(ctx context.Context) (CommandID, error)
}

func NewClient(ctx context.Context, uri string) (Client, error) {
	conn, err := connect.Connect(ctx, uri, nil)
	if err != nil {
		return nil, err
	}

	return &clientImpl{conn: conn}, nil
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
