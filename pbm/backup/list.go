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
	cur, err := m.Database(pbm.DB).Collection(pbm.BcpCollection).
		Find(ctx, bson.D{{"last_write_ts", bson.M{"$lt": ts}}})
	if err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	rv := []pbm.BackupMeta{}
	err = cur.All(ctx, &rv)
	return rv, errors.WithMessage(err, "cursor: all")
}

// ListSafeToDeleteBackups returns backups not required to restore to the ts.
// No more than requested backups will be deleted.
// It excludes the last incremental backups/chain if there is an incremental after the ts.
func ListSafeToDeleteBackups(ctx context.Context, m *mongo.Client, ts primitive.Timestamp) ([]pbm.BackupMeta, error) {
	backups, err := ListBackupsBefore(ctx, m, ts)
	if err != nil {
		return nil, err
	}

	return excludeLastIncrementalChain(ctx, m, backups)
}

func excludeLastIncrementalChain(ctx context.Context, m *mongo.Client, bcps []pbm.BackupMeta) ([]pbm.BackupMeta, error) {
	// lookup for the last incremental
	i := len(bcps) - 1
	for ; i >= 0; i-- {
		if bcps[i].Type == pbm.IncrementalBackup {
			break
		}
	}
	if i < 0 {
		// not found
		return bcps, nil
	}

	// check if there is an increment based on the backup
	res := m.Database(pbm.DB).Collection(pbm.BcpCollection).
		FindOne(ctx, bson.D{{"src_backup", bcps[i].Name}})
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			// the backup is the last increment in the chain. safe to delete
			return bcps, nil
		}
		return nil, errors.WithMessage(err, "query")
	}

	// exclude only the last incremental chain
	for base := bcps[i].Name; i >= 0; i-- {
		if bcps[i].Name != base {
			continue
		}
		base = bcps[i].SrcBackup

		// exclude the backup from slice by index
		copy(bcps[i:], bcps[i+1:])
		bcps = bcps[:len(bcps)-1]

		if base == "" {
			// the root/base of the chain
			break
		}
	}

	return bcps, nil
}
