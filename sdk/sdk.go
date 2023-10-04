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
	"github.com/percona/percona-backup-mongodb/internal/restore"
)

var ErrUnsupported = errors.New("unsupported")

type (
	CommandID string
	Timestamp = primitive.Timestamp
)

var NoOpID = CommandID(ctrl.NilOPID().String())

type BackupType = defs.BackupType

type (
	Config          = config.Config
	BackupMetadata  = backup.BackupMeta
	RestoreMetadata = restore.RestoreMeta
)

const (
	LogicalBackup     = defs.LogicalBackup
	PhysicalBackup    = defs.PhysicalBackup
	IncrementalBackup = defs.IncrementalBackup
	ExternalBackup    = defs.ExternalBackup
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

type CommandStatus interface {
	Ok() bool
	Done() bool
	Err() error
}

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

type Client interface {
	Close(ctx context.Context) error

	GetConfig(ctx context.Context) (*Config, error)

	GetAllBackups(ctx context.Context) ([]BackupMetadata, error)
	GetBackupByName(ctx context.Context, name string) (*BackupMetadata, error)

	GetRestoreByName(ctx context.Context, name string) (*RestoreMetadata, error)

	CancelBackup(ctx context.Context) (CommandID, error)

	SyncFromStorage(ctx context.Context) (CommandID, error)
}

func NewClient(ctx context.Context, uri string) (Client, error) {
	conn, err := connect.Connect(ctx, uri, nil)
	if err != nil {
		return nil, err
	}

	return &clientImpl{conn: conn}, nil
}
