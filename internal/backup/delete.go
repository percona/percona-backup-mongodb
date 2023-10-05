package backup

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/oplog"
	"github.com/percona/percona-backup-mongodb/internal/storage"
	"github.com/percona/percona-backup-mongodb/internal/util"
	"github.com/percona/percona-backup-mongodb/internal/version"
)

// DeleteBackup deletes backup with the given name from the current storage
// and pbm database
func DeleteBackup(ctx context.Context, conn connect.Client, name string, l log.LogEvent) error {
	meta, err := NewDBManager(conn).GetBackupByName(ctx, name)
	if err != nil {
		return errors.Wrap(err, "get backup meta")
	}

	tlns, err := oplog.PITRTimelines(ctx, conn)
	if err != nil {
		return errors.Wrap(err, "get PITR chunks")
	}

	err = probeDelete(ctx, conn, meta, tlns)
	if err != nil {
		return err
	}

	stg, err := util.GetStorage(ctx, conn, l)
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	err = DeleteBackupFiles(meta, stg)
	if err != nil {
		return errors.Wrap(err, "delete files from storage")
	}

	_, err = conn.BcpCollection().DeleteOne(ctx, bson.M{"name": meta.Name})
	if err != nil {
		return errors.Wrap(err, "delete metadata from db")
	}

	return nil
}

func probeDelete(ctx context.Context, m connect.Client, bcp *BackupMeta, tlns []oplog.Timeline) error {
	// check if backup isn't running
	switch bcp.Status {
	case defs.StatusDone, defs.StatusCancelled, defs.StatusError:
	default:
		return errors.Errorf("unable to delete backup in %s state", bcp.Status)
	}

	if bcp.Type == defs.ExternalBackup || util.IsSelective(bcp.Namespaces) {
		return nil
	}

	// if backup isn't a base for any PITR timeline
	for _, t := range tlns {
		if bcp.LastWriteTS.T == t.Start {
			return errors.Errorf("unable to delete: backup is a base for '%s'", t)
		}
	}

	ispitr, _, err := config.IsPITREnabled(ctx, m)
	if err != nil {
		return errors.Wrap(err, "unable check pitr state")
	}

	// if PITR is ON and there are no chunks yet we shouldn't delete the most recent back
	if !ispitr || tlns != nil {
		return nil
	}

	has, err := BackupHasNext(ctx, m, bcp)
	if err != nil {
		return errors.Wrap(err, "check next backup")
	}
	if !has {
		return errors.New("unable to delete the last backup while PITR is on")
	}

	return nil
}

// DeleteBackupFiles removes backup's artifacts from storage
func DeleteBackupFiles(meta *BackupMeta, stg storage.Storage) error {
	switch meta.Type {
	case defs.PhysicalBackup, defs.IncrementalBackup:
		return deletePhysicalBackupFiles(meta, stg)
	case defs.LogicalBackup:
		fallthrough
	default:
		var err error
		if version.IsLegacyArchive(meta.PBMVersion) {
			err = deleteLegacyLogicalBackupFiles(meta, stg)
		} else {
			err = deleteLogicalBackupFiles(meta, stg)
		}

		return err
	}
}

// DeleteBackupFiles removes backup's artifacts from storage
func deletePhysicalBackupFiles(meta *BackupMeta, stg storage.Storage) error {
	for _, r := range meta.Replsets {
		for _, f := range r.Files {
			fname := meta.Name + "/" + r.Name + "/" + f.Name + meta.Compression.Suffix()
			if f.Len != 0 {
				fname += fmt.Sprintf(".%d-%d", f.Off, f.Len)
			}
			err := stg.Delete(fname)
			if err != nil && !errors.Is(err, storage.ErrNotExist) {
				return errors.Wrapf(err, "delete %s", fname)
			}
		}
		for _, f := range r.Journal {
			fname := meta.Name + "/" + r.Name + "/" + f.Name + meta.Compression.Suffix()
			if f.Len != 0 {
				fname += fmt.Sprintf(".%d-%d", f.Off, f.Len)
			}
			err := stg.Delete(fname)
			if err != nil && !errors.Is(err, storage.ErrNotExist) {
				return errors.Wrapf(err, "delete %s", fname)
			}
		}
	}

	err := stg.Delete(meta.Name + defs.MetadataFileSuffix)
	if errors.Is(err, storage.ErrNotExist) {
		return nil
	}

	return errors.Wrap(err, "delete metadata file from storage")
}

// deleteLogicalBackupFiles removes backup's artifacts from storage
func deleteLogicalBackupFiles(meta *BackupMeta, stg storage.Storage) error {
	if stg.Type() == storage.Filesystem {
		return deleteLogicalBackupFilesFromFS(stg, meta.Name)
	}

	prefix := meta.Name + "/"
	files, err := stg.List(prefix, "")
	if err != nil {
		return errors.Wrapf(err, "get file list: %q", prefix)
	}

	eg := errgroup.Group{}
	for _, f := range files {
		ns := prefix + f.Name
		eg.Go(func() error {
			return errors.Wrapf(stg.Delete(ns), "delete %q", ns)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	bcpMF := meta.Name + defs.MetadataFileSuffix
	return errors.Wrapf(stg.Delete(bcpMF), "delete %q", bcpMF)
}

// deleteLogicalBackupFiles removes backup's artifacts from storage
func deleteLogicalBackupFilesFromFS(stg storage.Storage, bcpName string) error {
	if err := stg.Delete(bcpName); err != nil {
		return errors.Wrapf(err, "delete %q", bcpName)
	}

	bcpMetafile := bcpName + defs.MetadataFileSuffix
	return errors.Wrapf(stg.Delete(bcpMetafile), "delete %q", bcpMetafile)
}

// deleteLegacyLogicalBackupFiles removes backup's artifacts from storage
func deleteLegacyLogicalBackupFiles(meta *BackupMeta, stg storage.Storage) error {
	for _, r := range meta.Replsets {
		err := stg.Delete(r.OplogName)
		if err != nil && !errors.Is(err, storage.ErrNotExist) {
			return errors.Wrapf(err, "delete oplog %s", r.OplogName)
		}
		err = stg.Delete(r.DumpName)
		if err != nil && !errors.Is(err, storage.ErrNotExist) {
			return errors.Wrapf(err, "delete dump %s", r.DumpName)
		}
	}

	err := stg.Delete(meta.Name + defs.MetadataFileSuffix)
	if errors.Is(err, storage.ErrNotExist) {
		return nil
	}

	return errors.Wrap(err, "delete metadata file from storage")
}

// DeleteOlderThan deletes backups which older than given Time
func DeleteOlderThan(ctx context.Context, m connect.Client, t time.Time, l log.LogEvent) error {
	stg, err := util.GetStorage(ctx, m, l)
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	tlns, err := oplog.PITRTimelines(ctx, m)
	if err != nil {
		return errors.Wrap(err, "get PITR chunks")
	}

	cur, err := m.BcpCollection().Find(
		ctx,
		bson.M{
			"start_ts": bson.M{"$lt": t.Unix()},
		},
	)
	if err != nil {
		return errors.Wrap(err, "get backups list")
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		meta := &BackupMeta{}
		err := cur.Decode(meta)
		if err != nil {
			return errors.Wrap(err, "decode backup meta")
		}

		err = probeDelete(ctx, m, meta, tlns)
		if err != nil {
			l.Info("deleting %s: %v", meta.Name, err)
			continue
		}

		err = DeleteBackupFiles(meta, stg)
		if err != nil {
			return errors.Wrap(err, "delete backup files from storage")
		}

		_, err = m.BcpCollection().DeleteOne(ctx, bson.M{"name": meta.Name})
		if err != nil {
			return errors.Wrap(err, "delete backup meta from db")
		}
	}

	if cur.Err() != nil {
		return errors.Wrap(cur.Err(), "cursor")
	}

	return nil
}
