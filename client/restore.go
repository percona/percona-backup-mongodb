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

func restoreFromMeta(meta *pbm.RestoreMeta) *Restore {
	if meta == nil {
		return nil
	}

	return &Restore{Name: meta.Name}
}

func pitRestoreFromMeta(meta *pbm.RestoreMeta) *PITRestore {
	if meta == nil {
		return nil
	}

	return &PITRestore{Name: meta.Name}
}

func oplogReplayFromMeta(meta *pbm.RestoreMeta) *OplogReplay {
	if meta == nil {
		return nil
	}

	return &OplogReplay{Name: meta.Name}
}

func getRestoreMetadata(ctx context.Context, m *mongo.Client, name string) (*pbm.RestoreMeta, error) {
	res := coll(m, pbm.RestoresCollection).FindOne(ctx, bson.M{"name": name})
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, errors.WithMessage(err, "query")
	}

	meta := new(pbm.RestoreMeta)
	err := res.Decode(meta)
	return meta, errors.WithMessage(err, "decode")
}

func waitForRestoreMetadata(ctx context.Context, m *mongo.Client, name string) (*pbm.RestoreMeta, error) {
	t := time.NewTicker(time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
			meta, err := getRestoreMetadata(ctx, m, name)
			if err != nil {
				if errors.Is(err, ErrNotFound) {
					continue
				}
				return nil, err
			}

			return meta, nil
		}
	}
}

func runRestore(ctx context.Context, m *mongo.Client, opts *RestoreOptions) (primitive.ObjectID, string, error) {
	cmd := newCmd(pbm.CmdRestore)
	name := time.Unix(cmd.TS, 0).Format(time.RFC3339Nano)
	cmd.Restore = &pbm.RestoreCmd{
		Name:       name,
		BackupName: opts.BackupName,
		RSMap:      opts.RSMap,
	}

	id, err := sendCommand(ctx, m, cmd)
	return id, name, errors.WithMessage(err, "send command")
}

func runPITRestore(ctx context.Context, m *mongo.Client, opts *PITRestoreOptions) (primitive.ObjectID, string, error) {
	cmd := newCmd(pbm.CmdPITRestore)
	name := time.Unix(cmd.TS, 0).Format(time.RFC3339Nano)
	cmd.PITRestore = &pbm.PITRestoreCmd{
		Name:  name,
		TS:    int64(opts.Time.T),
		I:     int64(opts.Time.I),
		Bcp:   opts.Backup,
		RSMap: opts.RSMap,
	}

	id, err := sendCommand(ctx, m, cmd)
	return id, name, errors.WithMessage(err, "send command")
}

func runOplogReplay(ctx context.Context, m *mongo.Client, opts *OplogReplayOptions) (primitive.ObjectID, string, error) {
	cmd := newCmd(pbm.CmdReplay)
	name := time.Unix(cmd.TS, 0).Format(time.RFC3339Nano)
	cmd.Replay = &pbm.ReplayCmd{
		Name:  name,
		Start: opts.Start,
		End:   opts.End,
		RSMap: opts.RSMap,
	}

	id, err := sendCommand(ctx, m, cmd)
	return id, name, errors.WithMessage(err, "send command")
}
