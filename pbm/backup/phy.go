package backup

import (
	"context"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type uuid [16]byte

type Meta struct {
	ID           uuid                `bson:"backupId"`
	DBpath       string              `bson:"dbpath"`
	OplogStart   primitive.Timestamp `bson:"oplogStart"`
	OplogEnd     primitive.Timestamp `bson:"oplogEnd"`
	CheckpointTS primitive.Timestamp `bson:"checkpointTimestamp"`
}

type Data struct {
	File string `bson:"filename"`
	Size int64  `bson:"fileSize"`
}

type BCursorData struct {
	Meta *Meta
	Data []Data
}

func phyBackup(ctx context.Context, n *pbm.Node, l *plog.Event) (bcp *BCursorData, err error) {
	cur, err := n.Session().Database("admin").Aggregate(ctx, bson.D{{"$backupCursor", nil}})
	if err != nil {
		return nil, errors.Wrap(err, "create backupCursor")
	}
	defer func() {
		if err != nil {
			cur.Close(ctx)
		}
	}()

	var m *Meta
	var files []Data
	for cur.TryNext(ctx) {
		// metadata is the first
		if m == nil {
			mc := struct {
				data Meta `bson:"metadata"`
			}{}
			err = cur.Decode(&mc)
			if err != nil {
				return nil, errors.Wrap(err, "decode metadata")
			}
			m = &mc.data
			continue
		}

		var d Data
		err = cur.Decode(&d)
		if err != nil {
			return nil, errors.Wrap(err, "decode filename")
		}

		files = append(files, d)
	}

	go func() {
		for cur.Next(ctx) {
			time.Sleep(time.Minute)
		}
		l.Debug("stop cursor polling: %v",
			cur.Close(ctx))
	}()

	return &BCursorData{m, files}, nil
}

func phyJournals(n *pbm.Node, bcpID uuid, ts primitive.Timestamp) ([]string, error) {
	ctx := context.Background()
	cur, err := n.Session().Database("admin").Aggregate(ctx,
		bson.D{{"$backupCursorExtend", bson.M{"backupId": bcpID, "timestamp": ts}}})
	if err != nil {
		return nil, errors.Wrap(err, "create backupCursor")
	}
	defer cur.Close(ctx)

	var j []string

	err = cur.All(ctx, &j)
	return j, err
}
