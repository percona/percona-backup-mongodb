package oplogtmp

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/util"
)

// DeletePITR deletes backups which older than given `until` Time. It will round `until` down
// to the last write of the closest preceding backup in order not to make gaps. So usually it
// gonna leave some extra chunks. E.g. if `until = 13` and the last write of the closest preceding
// backup is `10` it will leave `11` and `12` chunks as well since `13` won't be restorable
// without `11` and `12` (contiguous timeline from the backup).
// It deletes all chunks if `until` is nil.
func DeletePITR(ctx context.Context, m connect.Client, until *time.Time, l log.LogEvent) error {
	stg, err := util.GetStorage(ctx, m, l)
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	var zerots primitive.Timestamp
	if until == nil {
		return deleteChunks(ctx, m, zerots, zerots, stg, l)
	}

	t := primitive.Timestamp{T: uint32(until.Unix()), I: 0}
	bcp, err := backup.GetLastBackup(ctx, m, &t)
	if errors.Is(err, errors.ErrNotFound) {
		return deleteChunks(ctx, m, zerots, t, stg, l)
	}

	if err != nil {
		return errors.Wrap(err, "get recent backup")
	}

	return deleteChunks(ctx, m, zerots, bcp.LastWriteTS, stg, l)
}

func deleteChunks(
	ctx context.Context,
	m connect.Client,
	start, until primitive.Timestamp,
	stg storage.Storage,
	l log.LogEvent,
) error {
	var chunks []oplog.OplogChunk

	var err error
	if until.T > 0 {
		chunks, err = oplog.PITRGetChunksSliceUntil(ctx, m, "", until)
	} else {
		chunks, err = oplog.PITRGetChunksSlice(ctx, m, "", start, until)
	}
	if err != nil {
		return errors.Wrap(err, "get pitr chunks")
	}
	if len(chunks) == 0 {
		l.Debug("nothing to delete")
	}

	for _, chnk := range chunks {
		err = stg.Delete(chnk.FName)
		if err != nil && !errors.Is(err, storage.ErrNotExist) {
			return errors.Wrapf(err, "delete pitr chunk '%s' (%v) from storage", chnk.FName, chnk)
		}

		_, err = m.PITRChunksCollection().DeleteOne(
			ctx,
			bson.D{
				{"rs", chnk.RS},
				{"start_ts", chnk.StartTS},
				{"end_ts", chnk.EndTS},
			},
		)

		if err != nil {
			return errors.Wrap(err, "delete pitr chunk metadata")
		}

		l.Debug("deleted %s", chnk.FName)
	}

	return nil
}
