package resync

import (
	"context"
	"encoding/json"
	"strings"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

// Resync sync oplog, backup, and restore meta from provided storage.
//
// It checks for read and write permissions, drops all meta from the database
// and populate it again by reading meta from the storage.
func Resync(ctx context.Context, conn connect.Client, stg storage.Storage) error {
	l := log.LogEventFromContext(ctx)

	err := storage.HasReadAccess(ctx, stg)
	if err != nil {
		if !errors.Is(err, storage.ErrUninitialized) {
			return errors.Wrap(err, "check read access")
		}

		err = storage.Initialize(ctx, stg)
		if err != nil {
			return errors.Wrap(err, "init storage")
		}
	} else {
		// check write permission and update PBM version
		err = storage.Reinitialize(ctx, stg)
		if err != nil {
			return errors.Wrap(err, "reinit storage")
		}
	}

	err = resyncPhysicalRestores(ctx, conn, stg)
	if err != nil {
		l.Error("resync physical restore metadata")
	}

	err = resyncBackupList(ctx, conn, stg)
	if err != nil {
		l.Error("resync backup metadata")
	}

	err = resyncOplogRange(ctx, conn, stg)
	if err != nil {
		l.Error("resync oplog range")
	}

	return nil
}

func resyncBackupList(
	ctx context.Context,
	conn connect.Client,
	stg storage.Storage,
) error {
	l := log.LogEventFromContext(ctx)

	backupList, err := getAllBackupMetaFromStorage(ctx, stg)
	if err != nil {
		return errors.Wrap(err, "get all backups meta from the storage")
	}

	l.Debug("got backups list: %v", len(backupList))

	if len(backupList) == 0 {
		return nil
	}

	docs := make([]any, len(backupList))
	for i, m := range backupList {
		l.Debug("bcp: %v", m.Name)

		docs[i] = m
	}

	_, err = conn.BcpCollection().DeleteMany(ctx, bson.M{})
	if err != nil {
		return errors.Wrapf(err, "delete all backup meta from db")
	}

	_, err = conn.BcpCollection().InsertMany(ctx, docs)
	if err != nil {
		return errors.Wrap(err, "insert backups meta into db")
	}

	return nil
}

func resyncOplogRange(
	ctx context.Context,
	conn connect.Client,
	stg storage.Storage,
) error {
	l := log.LogEventFromContext(ctx)

	_, err := conn.PITRChunksCollection().DeleteMany(ctx, bson.M{})
	if err != nil {
		return errors.Wrapf(err, "clean up %s", defs.PITRChunksCollection)
	}

	pitrf, err := stg.List(defs.PITRfsPrefix, "")
	if err != nil {
		return errors.Wrap(err, "get list of pitr chunks")
	}
	if len(pitrf) == 0 {
		return nil
	}

	var pitr []interface{}
	for _, f := range pitrf {
		stat, err := stg.FileStat(defs.PITRfsPrefix + "/" + f.Name)
		if err != nil {
			l.Warning("skip pitr chunk %s/%s because of %v", defs.PITRfsPrefix, f.Name, err)
			continue
		}
		chnk := oplog.MakeChunkMetaFromFilepath(f.Name)
		if chnk != nil {
			chnk.Size = stat.Size
			pitr = append(pitr, chnk)
		}
	}

	if len(pitr) == 0 {
		return nil
	}

	_, err = conn.PITRChunksCollection().InsertMany(ctx, pitr)
	if err != nil {
		return errors.Wrap(err, "insert retrieved pitr meta")
	}

	return nil
}

func resyncPhysicalRestores(
	ctx context.Context,
	conn connect.Client,
	stg storage.Storage,
) error {
	restoreFiles, err := stg.List(defs.PhysRestoresDir, ".json")
	if err != nil {
		return errors.Wrap(err, "get physical restores list from the storage")
	}

	log.LogEventFromContext(ctx).
		Debug("got physical restores list: %v", len(restoreFiles))

	if len(restoreFiles) == 0 {
		return nil
	}

	restoreMeta, err := getAllRestoreMetaFromStorage(ctx, stg)
	if err != nil {
		return errors.Wrap(err, "get all restore meta from storage")
	}

	docs := make([]any, len(restoreMeta))
	for i, m := range restoreMeta {
		docs[i] = m
	}

	_, err = conn.RestoresCollection().DeleteMany(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "delete all documents")
	}

	_, err = conn.RestoresCollection().InsertMany(ctx, docs)
	if err != nil {
		return errors.Wrap(err, "insert restore meta into db")
	}

	return nil
}

func getAllBackupMetaFromStorage(
	ctx context.Context,
	stg storage.Storage,
) ([]*backup.BackupMeta, error) {
	l := log.LogEventFromContext(ctx)

	backupFiles, err := stg.List("", defs.MetadataFileSuffix)
	if err != nil {
		return nil, errors.Wrap(err, "get a backups list from the storage")
	}

	backupMeta := make([]*backup.BackupMeta, 0, len(backupFiles))
	for _, b := range backupFiles {
		d, err := stg.SourceReader(b.Name)
		if err != nil {
			l.Error("read meta for %v", b.Name)
			continue
		}

		var meta *backup.BackupMeta
		err = json.NewDecoder(d).Decode(&meta)
		d.Close()
		if err != nil {
			l.Error("unmarshal backup meta [%s]", b.Name)
			continue
		}

		err = backup.CheckBackupFiles(ctx, meta, stg)
		if err != nil {
			l.Warning("skip snapshot %s: %v", meta.Name, err)
			meta.Status = defs.StatusError
			meta.Err = err.Error()
		}

		backupMeta = append(backupMeta, meta)
	}

	return backupMeta, nil
}

func getAllRestoreMetaFromStorage(
	ctx context.Context,
	stg storage.Storage,
) ([]*restore.RestoreMeta, error) {
	l := log.LogEventFromContext(ctx)

	restoreMeta, err := stg.List(defs.PhysRestoresDir, ".json")
	if err != nil {
		return nil, errors.Wrap(err, "get physical restores list from the storage")
	}

	rv := make([]*restore.RestoreMeta, 0, len(restoreMeta))
	for _, file := range restoreMeta {
		filename := strings.TrimSuffix(file.Name, ".json")
		meta, err := restore.GetPhysRestoreMeta(filename, stg, l)
		if err != nil {
			l.Error("get restore meta from storage: %s: %v", file.Name, err)
			if meta == nil {
				continue
			}
		}

		rv = append(rv, meta)
	}

	return rv, nil
}
