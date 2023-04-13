package pbm

import (
	"context"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm/sel"
)

type CleanupInfo struct {
	Backups []BackupMeta `json:"backups"`
	Chunks  []OplogChunk `json:"chunks"`
}

func MakeCleanupInfo(ctx context.Context, m *mongo.Client, ts primitive.Timestamp) (CleanupInfo, error) {
	backups, err := listBackupsBefore(ctx, m, primitive.Timestamp{T: ts.T + 1})
	if err != nil {
		return CleanupInfo{}, errors.WithMessage(err, "list backups before")
	}

	exclude := true
	if l := len(backups) - 1; l != -1 && backups[l].LastWriteTS.T == ts.T {
		// there is a backup at the `ts`
		if backups[l].Status == StatusDone && !sel.IsSelective(backups[l].Namespaces) {
			// it can be used to fully restore data to the `ts` state.
			// no need to exclude any base snapshot and chunks before the `ts`
			exclude = false
		}
		// the backup is not considered to be deleted.
		// used only for `exclude` value
		backups = backups[:l]
	}

	// exclude the last incremental backups if it is required for following (after the `ts`)
	backups, err = extractLastIncrementalChain(ctx, m, backups)
	if err != nil {
		return CleanupInfo{}, errors.WithMessage(err, "extract last incremental chain")
	}

	chunks, err := listChunksBefore(ctx, m, ts)
	if err != nil {
		return CleanupInfo{}, errors.WithMessage(err, "list chunks before")
	}
	if !exclude {
		// all chunks can be deleted. there is a backup to fully restore data
		return CleanupInfo{Backups: backups, Chunks: chunks}, nil
	}

	// the following check is needed for "delete all" special case.
	// if there is no base snapshot after `ts` and PITR is running,
	// the last base snapshot before `ts` should be excluded.
	// otherwise, it is allowed to delete everything before `ts`
	ok, err := canDeleteBaseSnapshot(ctx, m, ts)
	if err != nil {
		return CleanupInfo{}, err
	}
	if !ok {
		return CleanupInfo{Backups: backups, Chunks: chunks}, nil
	}

	// the `baseIndex` could be the base snapshot index for PITR to the `ts`
	// or for currently running PITR
	baseIndex := findLastBaseSnapshotIndex(backups)
	if baseIndex == -1 {
		// nothing to keep
		return CleanupInfo{Backups: backups, Chunks: chunks}, nil
	}

	excluded := false
	origin := chunks
	chunks = []OplogChunk{}
	for i := range origin {
		if primitive.CompareTimestamp(backups[baseIndex].LastWriteTS, origin[i].EndTS) != -1 {
			chunks = append(chunks, origin[i])
		} else {
			excluded = true
		}
	}

	// if excluded is false, the last found base snapshot is not used for PITR
	// no need to keep it. otherwise, should be excluded
	if excluded {
		copy(backups[baseIndex:], backups[baseIndex+1:])
		backups = backups[:len(backups)-1]
	}

	return CleanupInfo{Backups: backups, Chunks: chunks}, nil
}

// listBackupsBefore returns backups with restore cluster time less than or equals to ts
func listBackupsBefore(ctx context.Context, m *mongo.Client, ts primitive.Timestamp) ([]BackupMeta, error) {
	f := bson.D{{"last_write_ts", bson.M{"$lt": ts}}}
	o := options.Find().SetSort(bson.D{{"last_write_ts", 1}})
	cur, err := m.Database(DB).Collection(BcpCollection).Find(ctx, f, o)
	if err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	rv := []BackupMeta{}
	err = cur.All(ctx, &rv)
	return rv, errors.WithMessage(err, "cursor: all")
}

func canDeleteBaseSnapshot(ctx context.Context, m *mongo.Client, lw primitive.Timestamp) (bool, error) {
	f := bson.D{
		{"last_write_ts", bson.M{"$gte": lw}},
		{"nss", nil},
		{"type", LogicalBackup},
		{"status", StatusDone},
	}
	o := options.FindOne().SetProjection(bson.D{{"last_write_ts", 1}})
	err := m.Database(DB).Collection(BcpCollection).FindOne(ctx, f, o).Err()
	if err == nil {
		// there is a base snapshot after `lw`
		return true, nil
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		// unexpected error
		return false, err
	}

	enabled, oplogOnly, err := isPITREnabled(ctx, m)
	if err != nil {
		return false, err
	}

	// no base snapshot after the `lw`.
	// the backup with restore time `lw` can be deleted only if it is not used by running PITR
	return !enabled || oplogOnly, nil
}

// listChunksBefore returns oplog chunks that contain an op at the ts
func listChunksBefore(ctx context.Context, m *mongo.Client, ts primitive.Timestamp) ([]OplogChunk, error) {
	f := bson.D{{"start_ts", bson.M{"$lt": ts}}}
	o := options.Find().SetSort(bson.D{{"start_ts", 1}})
	cur, err := m.Database(DB).Collection(PITRChunksCollection).Find(ctx, f, o)
	if err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	rv := []OplogChunk{}
	err = cur.All(ctx, &rv)
	return rv, errors.WithMessage(err, "cursor: all")
}

func extractLastIncrementalChain(ctx context.Context, m *mongo.Client, bcps []BackupMeta) ([]BackupMeta, error) {
	// lookup for the last incremental
	i := len(bcps) - 1
	for ; i != -1; i-- {
		if bcps[i].Type == IncrementalBackup {
			break
		}
	}
	if i == -1 {
		// not found
		return bcps, nil
	}

	// check if there is an increment based on the backup
	f := bson.D{{"src_backup", bcps[i].Name}}
	res := m.Database(DB).Collection(BcpCollection).FindOne(ctx, f)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			// the backup is the last increment in the chain
			err = nil
		}
		return bcps, errors.WithMessage(err, "query")
	}

	for base := bcps[i].Name; i != -1; i-- {
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

func findLastBaseSnapshotIndex(bcps []BackupMeta) int {
	for i := len(bcps) - 1; i != -1; i-- {
		if isBaseSnapshot(&bcps[i]) {
			return i
		}
	}

	return -1
}

func isBaseSnapshot(bcp *BackupMeta) bool {
	if bcp.Status != StatusDone {
		return false
	}
	if bcp.Type != LogicalBackup || sel.IsSelective(bcp.Namespaces) {
		return false
	}

	return true
}
