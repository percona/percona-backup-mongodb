package backup

import (
	"context"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
)

// ListBackupsBefore returns list of backups where restore cluster time is less than ts
func ListBackupsBefore(ctx context.Context, m *mongo.Client, ts primitive.Timestamp) ([]pbm.BackupMeta, error) {
	filter := bson.D{{"last_write_ts", bson.M{"$lt": ts}}}
	cur, err := m.Database(pbm.DB).Collection(pbm.BcpCollection).Find(ctx, filter)
	if err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	rv := []pbm.BackupMeta{}
	err = cur.All(ctx, &rv)
	return rv, errors.WithMessage(err, "cursor: all")
}
