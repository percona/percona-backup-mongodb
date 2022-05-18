package client

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func backupFromMetadata(meta *pbm.BackupMeta) *Backup {
	if meta == nil {
		return nil
	}

	rv := Backup{
		Name:   meta.Name,
		Status: meta.Status,
	}
	return &rv
}

func getBackupMetadata(ctx context.Context, m *mongo.Client, name string) (*pbm.BackupMeta, error) {
	res := coll(m, pbm.BcpCollection).FindOne(ctx, bson.M{"name": name})
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, errors.WithMessage(err, "query")
	}

	meta := new(pbm.BackupMeta)
	err := res.Decode(meta)
	return meta, errors.WithMessage(err, "decode")
}

func getAllBackupMetadata(ctx context.Context, m *mongo.Client) ([]*pbm.BackupMeta, error) {
	cur, err := coll(m, pbm.BcpCollection).Find(ctx, bson.D{})
	if err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	var rs []*pbm.BackupMeta
	err = cur.All(ctx, rs)
	return rs, errors.WithMessage(err, "cursor")
}

type waitForBackupCondFn func(*pbm.BackupMeta) bool

func waitForBackupMetadata(ctx context.Context, m *mongo.Client, name string, cond waitForBackupCondFn) (*pbm.BackupMeta, error) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
			meta, err := getBackupMetadata(ctx, m, name)
			if err != nil && !errors.Is(err, ErrNotFound) {
				return nil, err
			}

			if cond(meta) {
				return meta, nil
			}
		}
	}
}

func runBackup(ctx context.Context, m *mongo.Client, opts *BackupOptions) (primitive.ObjectID, string, error) {
	name := time.Now().UTC().Format(time.RFC3339)
	cmd := newCmd(pbm.CmdBackup)
	cmd.Backup = &pbm.BackupCmd{
		Type:             opts.Type,
		Name:             name,
		Compression:      opts.Compression,
		CompressionLevel: opts.CompressionLevel,
	}

	id, err := sendCommand(ctx, m, cmd)
	return id, name, errors.WithMessage(err, "send command")
}

func cancelBackup(ctx context.Context, m *mongo.Client) (primitive.ObjectID, error) {
	cmd := newCmd(pbm.CmdCancelBackup)
	id, err := sendCommand(ctx, m, cmd)
	return id, errors.WithMessage(err, "send command")
}

func deleteBackupByName(ctx context.Context, m *mongo.Client, name string) (primitive.ObjectID, error) {
	cmd := newCmd(pbm.CmdDeleteBackup)
	cmd.Delete = &pbm.DeleteBackupCmd{Backup: name}

	id, err := sendCommand(ctx, m, cmd)
	return id, errors.WithMessage(err, "send command")
}

func deletesBackupOlderThan(ctx context.Context, m *mongo.Client, opts *DeleteManyBackupsOptions) (primitive.ObjectID, error) {
	cmd := newCmd(pbm.CmdDeleteBackup)
	cmd.Delete = &pbm.DeleteBackupCmd{
		OlderThan: opts.OlderThan.UTC().Unix(),
	}

	id, err := sendCommand(ctx, m, cmd)
	return id, errors.WithMessage(err, "send command")
}

func deleteOplog(ctx context.Context, m *mongo.Client, opts *DeleteOplogOptions) error {
	cmd := newCmd(pbm.CmdDeletePITR)
	cmd.DeletePITR = &pbm.DeletePITRCmd{
		OlderThan: int64(opts.OlderThan.T),
	}

	_, err := sendCommand(ctx, m, cmd)
	return errors.WithMessage(err, "send command")
}
