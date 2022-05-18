package client

import (
	"context"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
)

type CmdID primitive.ObjectID

type Client struct {
	client *mongo.Client
}

type Options struct {
	URI     string
	AppName string
}

type Result interface {
	ID() CmdID
	Err() error
}

type Status struct{}

type StatusOptions struct{}

type CmdResult interface {
	Result
	Done() <-chan struct{}
}

type SetConfigResult struct {
	cfg *pbm.Config
	id  CmdID
	err error
}

func (r *SetConfigResult) Config() *pbm.Config { return r.cfg }

func (r *SetConfigResult) ID() CmdID  { return r.id }
func (r *SetConfigResult) Err() error { return r.err }

type Backup struct {
	Name   string
	Status pbm.Status
}

type BackupOptions struct {
	Type             pbm.BackupType
	Compression      pbm.CompressionType
	CompressionLevel *int
}

type BackupResult struct {
	*Backup
	id  CmdID
	err error
}

func (r *BackupResult) ID() CmdID  { return r.id }
func (r *BackupResult) Err() error { return r.err }

type CancelBackupResult struct {
	id  CmdID
	err error
}

func (r *CancelBackupResult) ID() CmdID  { return r.id }
func (r *CancelBackupResult) Err() error { return r.err }

type DeleteManyBackupsOptions struct {
	OlderThan time.Time
}

type DeleteBackupResult struct {
	id  CmdID
	err error
}

func (r *DeleteBackupResult) ID() CmdID  { return r.id }
func (r *DeleteBackupResult) Err() error { return r.err }

type Restore struct {
	Name string
}

type RestoreOptions struct {
	BackupName string
	RSMap      map[string]string
}

type RestoreResult struct {
	*Restore
	id  CmdID
	err error
}

func (r *RestoreResult) ID() CmdID  { return r.id }
func (r *RestoreResult) Err() error { return r.err }

type PITRestore struct {
	Name   string
	Backup string
	Time   primitive.Timestamp
	RSMap  map[string]string
}

type PITRestoreOptions struct {
	Backup string
	Time   primitive.Timestamp
	RSMap  map[string]string
}

type PITRestoreResult struct {
	*PITRestore
	id  CmdID
	err error
}

func (r *PITRestoreResult) ID() CmdID  { return r.id }
func (r *PITRestoreResult) Err() error { return r.err }

type OplogReplay struct {
	Name string
}

type OplogReplayOptions struct {
	Start primitive.Timestamp
	End   primitive.Timestamp
	RSMap map[string]string
}

type OplogReplayResult struct {
	*OplogReplay
	id  CmdID
	err error
}

func (r *OplogReplayResult) ID() CmdID  { return r.id }
func (r *OplogReplayResult) Err() error { return r.err }

type DeleteOplogOptions struct {
	OlderThan primitive.Timestamp
}

type DeleteOplogResult struct {
	err error
}

func (r *DeleteOplogResult) ID() CmdID  { return CmdID(primitive.NilObjectID) }
func (r *DeleteOplogResult) Err() error { return r.err }

func New(ctx context.Context, opts Options) (*Client, error) {
	uri, err := lookupLeaderURI(ctx, opts.URI, opts.AppName)
	if err != nil {
		return nil, errors.WithMessage(err, "lookup leader uri")
	}

	client, err := connect(ctx, uri, opts.AppName)
	if err != nil {
		return nil, errors.WithMessage(err, "connect")
	}

	return &Client{client: client}, nil
}

type Shard struct {
	ID   string
	RS   string
	Host string
}

func (c *Client) GetStatus(ctx context.Context, opts *StatusOptions) (*Status, error) {
	info, err := nodeInfo(ctx, c.client)
	if err != nil {
		return nil, err
	}

	shards := []Shard{{
		RS:   info.SetName,
		Host: info.SetName + "/" + strings.Join(info.Hosts, ","),
	}}
	if info.IsSharded() {
		cur, err := c.client.Database("config").Collection("shards").Find(ctx, bson.M{})
		if err != nil {
			return nil, errors.Wrap(err, "query")
		}

		defer cur.Close(context.Background())

		for cur.Next(ctx) {
			s := Shard{}
			err := cur.Decode(&s)
			if err != nil {
				return nil, errors.Wrap(err, "decode")
			}
			s.RS = s.ID
			// _id may differ from the rs name, so extract rs name from the host (format like "rs2/localhost:27017")
			// see https://jira.percona.com/browse/PBM-595
			h := strings.Split(s.Host, "/")
			if len(h) > 1 {
				s.RS = h[0]
			}
			shards = append(shards, s)
		}
	}

	return nil, nil
}

func (c *Client) GetConfig(ctx context.Context) (*pbm.Config, error) {
	return getConfig(ctx, c.client)
}

func (c *Client) SetConfig(ctx context.Context, config *pbm.Config, opts *ConfigOptions) *SetConfigResult {
	// todo: validate

	id, err := setConfig(ctx, c.client, config, opts)
	if err != nil {
		return &SetConfigResult{
			id:  CmdID(id),
			err: errors.WithMessage(err, "run"),
		}
	}

	err = waitForCmdStatus(ctx, c.client, id, pbm.StatusDone, pbm.StatusError)
	rv := SetConfigResult{
		id:  CmdID(id),
		err: errors.WithMessage(err, "wait"),
	}
	return &rv
}

func (c *Client) GetBackup(ctx context.Context, name string) (*Backup, error) {
	meta, err := getBackupMetadata(ctx, c.client, name)
	return backupFromMetadata(meta), err
}

func (c *Client) GetAllBackups(ctx context.Context) ([]Backup, error) {
	rs, err := getAllBackupMetadata(ctx, c.client)
	if err != nil {
		return nil, err
	}

	backups := make([]Backup, len(rs))
	for i := range rs {
		backups[i] = *backupFromMetadata(rs[i])
	}

	return backups, nil
}

func (c *Client) Backup(ctx context.Context, opts *BackupOptions) *BackupResult {
	id, name, err := runBackup(ctx, c.client, opts)
	if err != nil {
		return &BackupResult{err: errors.WithMessage(err, "run")}
	}

	tillStarted := func(meta *pbm.BackupMeta) bool { return meta != nil }
	meta, err := waitForBackupMetadata(ctx, c.client, name, tillStarted)
	rv := BackupResult{
		id:     CmdID(id),
		Backup: backupFromMetadata(meta),
		err:    errors.WithMessage(err, "wait"),
	}
	return &rv
}

func (c *Client) CancelBackup(ctx context.Context) *CancelBackupResult {
	id, err := cancelBackup(ctx, c.client)
	return &CancelBackupResult{id: CmdID(id), err: err}
}

func (c *Client) DeleteBackup(ctx context.Context, name string) *DeleteBackupResult {
	id, err := deleteBackupByName(ctx, c.client, name)
	return &DeleteBackupResult{id: CmdID(id), err: err}
}

func (c *Client) DeleteManyBackups(ctx context.Context, opts *DeleteManyBackupsOptions) *DeleteBackupResult {
	id, err := deletesBackupOlderThan(ctx, c.client, opts)
	return &DeleteBackupResult{id: CmdID(id), err: err}
}

func (c *Client) Restore(ctx context.Context, opts *RestoreOptions) *RestoreResult {
	id, name, err := runRestore(ctx, c.client, opts)
	if err != nil {
		return &RestoreResult{err: errors.WithMessage(err, "run")}
	}

	meta, err := waitForRestoreMetadata(ctx, c.client, name)
	rv := RestoreResult{
		id:      CmdID(id),
		Restore: restoreFromMeta(meta),
		err:     errors.WithMessage(err, "wait"),
	}
	return &rv
}

func (c *Client) RestorePIT(ctx context.Context, opts *PITRestoreOptions) *PITRestoreResult {
	id, name, err := runPITRestore(ctx, c.client, opts)
	if err != nil {
		return &PITRestoreResult{err: errors.WithMessage(err, "run")}
	}

	meta, err := waitForRestoreMetadata(ctx, c.client, name)
	rv := PITRestoreResult{
		id:         CmdID(id),
		PITRestore: pitRestoreFromMeta(meta),
		err:        errors.WithMessage(err, "wait"),
	}
	return &rv
}

func (c *Client) ReplayOplog(ctx context.Context, opts *OplogReplayOptions) *OplogReplayResult {
	id, name, err := runOplogReplay(ctx, c.client, opts)
	if err != nil {
		return &OplogReplayResult{err: errors.WithMessage(err, "run")}
	}

	meta, err := waitForRestoreMetadata(ctx, c.client, name)
	rv := OplogReplayResult{
		id:          CmdID(id),
		OplogReplay: oplogReplayFromMeta(meta),
		err:         errors.WithMessage(err, "wait"),
	}
	return &rv
}

func (c *Client) DeleteOplog(ctx context.Context, opts *DeleteOplogOptions) *DeleteOplogResult {
	err := deleteOplog(ctx, c.client, opts)
	return &DeleteOplogResult{err: err}
}

func WaitForBackupFinish(ctx context.Context, c *Client, name string) (*Backup, error) {
	tillFinished := func(meta *pbm.BackupMeta) bool {
		return meta != nil && meta.Status.Finished()
	}
	meta, err := waitForBackupMetadata(ctx, c.client, name, tillFinished)
	return backupFromMetadata(meta), err
}

func WaitForDeleteBackupFinish(ctx context.Context, c *Client, id CmdID) error {
	// return waitForOp(ctx, c.client, pbm.CmdDeleteBackup)
	return waitForCmdStatus(ctx, c.client, primitive.ObjectID(id), pbm.StatusDone, pbm.StatusError)
}

func WaitForDeleteOplogFinish(ctx context.Context, c *Client, id CmdID) error {
	// return waitForOp(ctx, c.client, pbm.CmdDeletePITR)
	return waitForCmdStatus(ctx, c.client, primitive.ObjectID(id), pbm.StatusDone, pbm.StatusError)
}

func WaitForCancelBackup(ctx context.Context, c *Client, id CmdID) error {
	return waitForCmdStatus(ctx, c.client, primitive.ObjectID(id), pbm.StatusDone, pbm.StatusError)
}
