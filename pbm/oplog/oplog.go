package oplog

import (
	"context"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
)

// ListChunksBefore returns oplog chunks
func ListChunksBefore(ctx context.Context, m *mongo.Client, ts primitive.Timestamp) ([]pbm.OplogChunk, error) {
	filter := bson.D{{"end_ts", bson.M{"$lt": ts}}}
	cur, err := m.Database(pbm.DB).Collection(pbm.PITRChunksCollection).Find(ctx, filter)
	if err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	rv := []pbm.OplogChunk{}
	err = cur.All(ctx, &rv)
	return rv, errors.WithMessage(err, "cursor: all")
}
