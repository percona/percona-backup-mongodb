package backup

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/oplog"
	"github.com/percona/percona-backup-mongodb/internal/util"
)

const SelectiveBackup defs.BackupType = "selective"

var (
	errBackupInProgress     = errors.New("backup is in progress")
	errSourceForIncremental = errors.New("the backup required for following incremental")
	errBaseForPITR          = errors.New("unable to delete the last backup while PITR is enabled")
	errNothing              = errors.New("nothing")
)

type CleanupInfo struct {
	Backups []BackupMeta       `json:"backups"`
	Chunks  []oplog.OplogChunk `json:"chunks"`
}

// DeleteBackup deletes backup with the given name from the current storage
// and pbm database
func DeleteBackup(ctx context.Context, cc connect.Client, name string) error {
	meta, err := NewDBManager(cc).GetBackupByName(ctx, name)
	if err != nil {
		return errors.Wrap(err, "get backup meta")
	}

	err = CanDeleteBackup(ctx, cc, meta)
	if err != nil {
		return err
	}

	stg, err := util.GetStorage(ctx, cc, log.LogEventFromContext(ctx))
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	err = DeleteBackupFiles(meta, stg)
	if err != nil {
		return errors.Wrap(err, "delete files from storage")
	}

	_, err = cc.BcpCollection().DeleteOne(ctx, bson.M{"name": meta.Name})
	if err != nil {
		return errors.Wrap(err, "delete metadata from db")
	}

	return nil
}

func CanDeleteBackup(ctx context.Context, cc connect.Client, bcp *BackupMeta) error {
	if bcp.Status.IsRunning() {
		return errBackupInProgress
	}
	if !IsValidBaseSnapshot(bcp) {
		return nil
	}

	isSource, err := IsSourceForIncremental(ctx, cc, bcp.Name)
	if err != nil {
		return errors.Wrap(err, "check source incremental")
	}
	if isSource {
		return errSourceForIncremental
	}

	ok, err := IsNeededForPITR(ctx, cc, bcp.LastWriteTS)
	if err != nil {
		return errors.Wrap(err, "check pitr requirements")
	}
	if !ok {
		return errBaseForPITR
	}

	return nil
}

func IsSourceForIncremental(ctx context.Context, cc connect.Client, bcpName string) (bool, error) {
	// check if there is an increment based on the backup
	f := bson.D{
		{"src_backup", bcpName},
		{"status", defs.StatusDone},
	}
	res := cc.BcpCollection().FindOne(ctx, f)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			// the backup is the last increment in the chain
			return false, nil
		}
		return false, errors.Wrap(err, "query")
	}

	return true, nil
}

func IsValidBaseSnapshot(bcp *BackupMeta) bool {
	if bcp.Status != defs.StatusDone {
		return false
	}
	if bcp.Type == defs.ExternalBackup {
		return false
	}
	if util.IsSelective(bcp.Namespaces) {
		return false
	}

	return true
}

func IsNeededForPITR(ctx context.Context, cc connect.Client, lw primitive.Timestamp) (bool, error) {
	enabled, oplogOnly, err := config.IsPITREnabled(ctx, cc)
	if err != nil {
		return false, err
	}

	// the backup with restore time `lw` can be deleted only if it is not used by running PITR
	if !enabled || oplogOnly {
		return true, nil
	}

	has, err := IsBaseSnapshotAfter(ctx, cc, lw)
	if err != nil {
		return false, errors.Wrap(err, "check next base snapshot")
	}

	return has, nil
}

// DeleteOlderThan deletes backups which older than given Time
func DeleteOlderThan(ctx context.Context, cc connect.Client, t time.Time, bcpType defs.BackupType) error {
	info, err := MakeCleanupInfo(ctx, cc, primitive.Timestamp{T: uint32(t.Unix())})
	if err != nil {
		return err
	}
	if len(info.Backups) == 0 {
		return errNothing
	}

	stg, err := util.GetStorage(ctx, cc, log.LogEventFromContext(ctx))
	if err != nil {
		return errors.Wrap(err, "get storage")
	}
	backups := filterBackupsByType(info.Backups, bcpType)
	if len(backups) == 0 {
		return errNothing
	}

	for i := range backups {
		bcp := &backups[i]

		err := DeleteBackupFiles(bcp, stg)
		if err != nil {
			return errors.Wrapf(err, "delete files from storage for %q", bcp.Name)
		}

		_, err = cc.BcpCollection().DeleteOne(ctx, bson.M{"name": bcp.Name})
		if err != nil {
			return errors.Wrapf(err, "delete metadata from db for %q", bcp.Name)
		}
	}

	return nil
}

func filterBackupsByType(backups []BackupMeta, t defs.BackupType) []BackupMeta {
	rv := []BackupMeta{}

	pred := func(m *BackupMeta) bool { return m.Type == t }
	if t == SelectiveBackup {
		pred = func(m *BackupMeta) bool { return util.IsSelective(m.Namespaces) }
	}

	for i := range backups {
		if pred(&backups[i]) {
			rv = append(rv, backups[i])
		}
	}

	return rv
}

func MakeCleanupInfo(ctx context.Context, conn connect.Client, ts primitive.Timestamp) (CleanupInfo, error) {
	backups, err := listBackupsBefore(ctx, conn, primitive.Timestamp{T: ts.T + 1})
	if err != nil {
		return CleanupInfo{}, errors.Wrap(err, "list backups before")
	}

	exclude := true
	if l := len(backups) - 1; l != -1 && backups[l].LastWriteTS.T == ts.T {
		// there is a backup at the `ts`
		if backups[l].Status == defs.StatusDone && !util.IsSelective(backups[l].Namespaces) {
			// it can be used to fully restore data to the `ts` state.
			// no need to exclude any base snapshot and chunks before the `ts`
			exclude = false
		}
		// the backup is not considered to be deleted.
		// used only for `exclude` value
		backups = backups[:l]
	}

	// exclude the last incremental backups if it is required for following (after the `ts`)
	backups, err = extractLastIncrementalChain(ctx, conn, backups)
	if err != nil {
		return CleanupInfo{}, errors.Wrap(err, "extract last incremental chain")
	}

	chunks, err := listChunksBefore(ctx, conn, ts)
	if err != nil {
		return CleanupInfo{}, errors.Wrap(err, "list chunks before")
	}
	if !exclude {
		// all chunks can be deleted. there is a backup to fully restore data
		return CleanupInfo{Backups: backups, Chunks: chunks}, nil
	}

	// the following check is needed for "delete all" special case.
	// if there is no base snapshot after `ts` and PITR is running,
	// the last base snapshot before `ts` should be excluded.
	// otherwise, it is allowed to delete everything before `ts`
	ok, err := IsNeededForPITR(ctx, conn, ts)
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
	chunks = []oplog.OplogChunk{}
	for i := range origin {
		if backups[baseIndex].LastWriteTS.Compare(origin[i].EndTS) != -1 {
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
func listBackupsBefore(ctx context.Context, conn connect.Client, ts primitive.Timestamp) ([]BackupMeta, error) {
	f := bson.D{{"last_write_ts", bson.M{"$lt": ts}}}
	o := options.Find().SetSort(bson.D{{"last_write_ts", 1}})
	cur, err := conn.BcpCollection().Find(ctx, f, o)
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}

	rv := []BackupMeta{}
	err = cur.All(ctx, &rv)
	return rv, errors.Wrap(err, "cursor: all")
}

// listChunksBefore returns oplog chunks that contain an op at the ts
func listChunksBefore(ctx context.Context, conn connect.Client, ts primitive.Timestamp) ([]oplog.OplogChunk, error) {
	f := bson.D{{"start_ts", bson.M{"$lt": ts}}}
	o := options.Find().SetSort(bson.D{{"start_ts", 1}})
	cur, err := conn.PITRChunksCollection().Find(ctx, f, o)
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}

	rv := []oplog.OplogChunk{}
	err = cur.All(ctx, &rv)
	return rv, errors.Wrap(err, "cursor: all")
}

func extractLastIncrementalChain(
	ctx context.Context,
	conn connect.Client,
	bcps []BackupMeta,
) ([]BackupMeta, error) {
	// lookup for the last incremental
	i := len(bcps) - 1
	for ; i != -1; i-- {
		if bcps[i].Type == defs.IncrementalBackup {
			break
		}
	}
	if i == -1 {
		// not found
		return bcps, nil
	}

	isSource, err := IsSourceForIncremental(ctx, conn, bcps[i].Name)
	if err != nil {
		return bcps, err
	}
	if !isSource {
		return bcps, nil
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
		if IsValidBaseSnapshot(&bcps[i]) {
			return i
		}
	}

	return -1
}
