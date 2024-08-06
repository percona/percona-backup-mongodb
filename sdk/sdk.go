package sdk

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/log"
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
	GetAllConfigProfiles(ctx context.Context) ([]config.Config, error)
	GetConfigProfile(ctx context.Context, name string) (*config.Config, error)
	AddConfigProfile(ctx context.Context, name string, cfg *config.Config) (CommandID, error)
	RemoveConfigProfile(ctx context.Context, name string) (CommandID, error)

	GetAllBackups(ctx context.Context) ([]BackupMetadata, error)
	GetBackupByName(ctx context.Context, name string, options GetBackupByNameOptions) (*BackupMetadata, error)

	GetAllRestores(ctx context.Context, m connect.Client, options GetAllRestoresOptions) ([]RestoreMetadata, error)
	GetRestoreByName(ctx context.Context, name string) (*RestoreMetadata, error)

	CancelBackup(ctx context.Context) (CommandID, error)

	DeleteBackupByName(ctx context.Context, name string) (CommandID, error)
	DeleteBackupBefore(ctx context.Context, beforeTS Timestamp, options DeleteBackupBeforeOptions) (CommandID, error)

	DeleteOplogRange(ctx context.Context, until Timestamp) (CommandID, error)

	CleanupReport(ctx context.Context, beforeTS Timestamp) (CleanupReport, error)
	RunCleanup(ctx context.Context, beforeTS Timestamp) (CommandID, error)

	SyncFromStorage(ctx context.Context) (CommandID, error)
	SyncFromExternalStorage(ctx context.Context, name string) (CommandID, error)
	SyncFromAllExternalStorages(ctx context.Context) (CommandID, error)
	ClearSyncFromExternalStorage(ctx context.Context, name string) (CommandID, error)
	ClearSyncFromAllExternalStorages(ctx context.Context) (CommandID, error)
}

func NewClient(ctx context.Context, uri string) (*clientImpl, error) {
	conn, err := connect.Connect(ctx, uri, "sdk")
	if err != nil {
		return nil, err
	}

	return &clientImpl{conn: conn}, nil
}

func WaitForCommandWithErrorLog(ctx context.Context, client Client, cid CommandID) error {
	err := waitOp(ctx, client.(*clientImpl).conn, &lock.LockHeader{OPID: string(cid)})
	if err != nil {
		return err
	}

	errorMessage, err := log.CommandLastError(ctx, client.(*clientImpl).conn, string(cid))
	if err != nil {
		return fmt.Errorf("read error log: %w", err)
	}
	if errorMessage != "" {
		return errors.New(errorMessage) //nolint:err113
	}

	return nil
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
