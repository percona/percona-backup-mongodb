package sdk

import (
	"context"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
)

var (
	ErrUnsupported      = errors.New("unsupported")
	ErrInvalidCommandID = errors.New("invalid command id")
	ErrNotFound         = errors.New("not found")
)

type (
	Command     = ctrl.Cmd
	CommandID   string
	CommandType = ctrl.Command
	Timestamp   = primitive.Timestamp
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

// OpLock represents internal PBM lock.
//
// Some commands can have many locks (one lock per replset).
type OpLock struct {
	// OpID is its command id.
	OpID CommandID `json:"opid,omitempty"`
	// Cmd is the type of command
	Cmd CommandType `json:"cmd,omitempty"`
	// Replset is name of a replset that acquired the lock.
	Replset string `json:"rs,omitempty"`
	// Node is `host:port` pair of an agent that acquired the lock.
	Node string `json:"node,omitempty"`
	// Heartbeat is the last cluster time seen by an agent that acquired the lock.
	Heartbeat primitive.Timestamp `json:"hb"`

	err error
}

func (l *OpLock) Err() error {
	return l.err
}

func NewClient(ctx context.Context, uri string) (*Client, error) {
	conn, err := connect.Connect(ctx, uri, "sdk")
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

func WaitForCleanup(ctx context.Context, client *Client) error {
	lck := &lock.LockHeader{Type: ctrl.CmdCleanup}
	return waitOp(ctx, client.conn, lck)
}

func WaitForDeleteBackup(ctx context.Context, client *Client) error {
	lck := &lock.LockHeader{Type: ctrl.CmdDeleteBackup}
	return waitOp(ctx, client.conn, lck)
}

func WaitForDeleteOplogRange(ctx context.Context, client *Client) error {
	lck := &lock.LockHeader{Type: ctrl.CmdDeletePITR}
	return waitOp(ctx, client.conn, lck)
}

func WaitForErrorLog(ctx context.Context, client *Client, cmd *Command) (string, error) {
	return lastLogErr(ctx, client.conn, cmd.Cmd, cmd.TS)
}

func WaitForResync(ctx context.Context, c *Client, cid CommandID) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	r := &log.LogRequest{
		LogKeys: log.LogKeys{
			Event:    string(ctrl.CmdResync),
			OPID:     string(cid),
			Severity: log.Info,
		},
	}

	outC, errC := log.Follow(ctx, c.conn, r, false)

	for {
		select {
		case entry := <-outC:
			if entry != nil && entry.Msg == "succeed" {
				return nil
			}
		case err := <-errC:
			return err
		}
	}
}

func CanDeleteBackup(ctx context.Context, client *Client, bcp *BackupMetadata) error {
	return backup.CanDeleteBackup(ctx, client.conn, bcp)
}

func CanDeleteIncrementalBackup(
	ctx context.Context,
	client *Client,
	bcp *BackupMetadata,
	increments [][]*BackupMetadata,
) error {
	return backup.CanDeleteIncrementalChain(ctx, client.conn, bcp, increments)
}

func ListDeleteBackupBefore(
	ctx context.Context,
	client *Client,
	ts primitive.Timestamp,
	bcpType BackupType,
) ([]BackupMetadata, error) {
	return backup.ListDeleteBackupBefore(ctx, client.conn, ts, bcpType)
}

func ListDeleteChunksBefore(
	ctx context.Context,
	client *Client,
	ts primitive.Timestamp,
) ([]OplogChunk, error) {
	r, err := backup.MakeCleanupInfo(ctx, client.conn, ts)
	return r.Chunks, err
}

func ParseDeleteBackupType(s string) (BackupType, error) {
	return backup.ParseDeleteBackupType(s)
}
