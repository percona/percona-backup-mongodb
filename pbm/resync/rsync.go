package resync

import (
	"context"
	"encoding/json"
	"runtime"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/util"
)

// Resync sync oplog, backup, and restore meta from provided storage.
//
// It checks for read and write permissions, drops all meta from the database
// and populate it again by reading meta from the storage.
func Resync(ctx context.Context, conn connect.Client, cfg *config.Storage) error {
	l := log.LogEventFromContext(ctx)

	stg, err := util.StorageFromConfig(cfg, l)
	if err != nil {
		return errors.Wrap(err, "unable to get backup store")
	}

	err = storage.HasReadAccess(ctx, stg)
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

	err = SyncBackupList(ctx, conn, cfg, "")
	if err != nil {
		l.Error("resync backup metadata")
	}

	err = resyncOplogRange(ctx, conn, stg)
	if err != nil {
		l.Error("resync oplog range")
	}

	return nil
}

func ClearBackupList(ctx context.Context, conn connect.Client, profile string) error {
	var filter bson.D
	if profile == "" {
		// from main storage
		filter = bson.D{
			{"store.profile", nil},
		}
	} else {
		filter = bson.D{
			{"store.profile", true},
			{"store.name", profile},
		}
	}

	_, err := conn.BcpCollection().DeleteMany(ctx, filter)
	if err != nil {
		return errors.Wrapf(err, "delete all backup meta from db")
	}

	return nil
}

func SyncBackupList(
	ctx context.Context,
	conn connect.Client,
	cfg *config.Storage,
	profile string,
) error {
	l := log.LogEventFromContext(ctx)

	stg, err := util.StorageFromConfig(cfg, l)
	if err != nil {
		return errors.Wrap(err, "storage from config")
	}

	err = ClearBackupList(ctx, conn, profile)
	if err != nil {
		return errors.Wrapf(err, "clear backup list")
	}

	backupList, err := getAllBackupMetaFromStorage(ctx, stg)
	if err != nil {
		return errors.Wrap(err, "get all backups meta from the storage")
	}

	l.Debug("got backups list: %v", len(backupList))

	if len(backupList) == 0 {
		return nil
	}

	backupStore := backup.Storage{
		Name:      profile,
		IsProfile: profile != "",
		Storage:   *cfg,
	}

	for i := range backupList {
		// overwriting config allows PBM to download files from the current deployment
		backupList[i].Store = backupStore
	}

	return insertBackupList(ctx, conn, backupList)
}

func insertBackupList(
	ctx context.Context,
	conn connect.Client,
	backups []*backup.BackupMeta,
) error {
	concurrencyNumber := runtime.NumCPU()

	inC := make(chan *backup.BackupMeta)
	errC := make(chan error, concurrencyNumber)

	wg := &sync.WaitGroup{}
	wg.Add(concurrencyNumber)
	for range concurrencyNumber {
		go func() {
			defer wg.Done()
			l := log.LogEventFromContext(ctx)

			for bcp := range inC {
				l.Debug("bcp: %v", bcp.Name)

				_, err := conn.BcpCollection().InsertOne(ctx, bcp)
				if err != nil {
					if mongo.IsDuplicateKeyError(err) {
						l.Warning("backup %q already exists", bcp.Name)
						continue
					}
					errC <- errors.Wrapf(err, "backup %q", bcp.Name)
				}
			}
		}()
	}

	go func() {
		for _, bcp := range backups {
			inC <- bcp
		}

		close(inC)
		wg.Wait()
		close(errC)
	}()

	var errs []error
	for err := range errC {
		errs = append(errs, err)
	}
	if len(errs) != 0 {
		return errors.Errorf("write backup meta:\n%v", errors.Join(errs...))
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

	chunkFiles, err := stg.List(defs.PITRfsPrefix, "")
	if err != nil {
		return errors.Wrap(err, "get list of pitr chunks")
	}

	var chunks []any
	for _, file := range chunkFiles {
		info, err := stg.FileStat(defs.PITRfsPrefix + "/" + file.Name)
		if err != nil {
			l.Warning("skip pitr chunk %s/%s because of %v", defs.PITRfsPrefix, file.Name, err)
			continue
		}

		chunk := oplog.MakeChunkMetaFromFilepath(file.Name)
		if chunk != nil {
			chunk.Size = info.Size
			chunks = append(chunks, chunk)
		}
	}

	if len(chunks) == 0 {
		return nil
	}

	_, err = conn.PITRChunksCollection().InsertMany(ctx, chunks)
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
	_, err := conn.RestoresCollection().DeleteMany(ctx, bson.D{})
	if err != nil {
		return errors.Wrap(err, "delete all documents")
	}

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
