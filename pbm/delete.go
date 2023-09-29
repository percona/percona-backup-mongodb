package pbm

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/query"
	"github.com/percona/percona-backup-mongodb/internal/storage"
	"github.com/percona/percona-backup-mongodb/internal/types"
	"github.com/percona/percona-backup-mongodb/internal/util"
	"github.com/percona/percona-backup-mongodb/internal/version"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
)

// DeleteBackup deletes backup with the given name from the current storage
// and pbm database
func (p *PBM) DeleteBackup(ctx context.Context, name string, l *log.Event) error {
	meta, err := query.GetBackupMeta(ctx, p.Conn, name)
	if err != nil {
		return errors.Wrap(err, "get backup meta")
	}

	tlns, err := oplog.PITRTimelines(ctx, p.Conn)
	if err != nil {
		return errors.Wrap(err, "get PITR chunks")
	}

	err = p.probeDelete(ctx, meta, tlns)
	if err != nil {
		return err
	}

	stg, err := util.GetStorage(ctx, p.Conn, l)
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	err = p.DeleteBackupFiles(meta, stg)
	if err != nil {
		return errors.Wrap(err, "delete files from storage")
	}

	_, err = p.Conn.BcpCollection().DeleteOne(ctx, bson.M{"name": meta.Name})
	if err != nil {
		return errors.Wrap(err, "delete metadata from db")
	}

	return nil
}

func (p *PBM) probeDelete(ctx context.Context, backup *types.BackupMeta, tlns []oplog.Timeline) error {
	// check if backup isn't running
	switch backup.Status {
	case defs.StatusDone, defs.StatusCancelled, defs.StatusError:
	default:
		return errors.Errorf("unable to delete backup in %s state", backup.Status)
	}

	if backup.Type == defs.ExternalBackup || util.IsSelective(backup.Namespaces) {
		return nil
	}

	// if backup isn't a base for any PITR timeline
	for _, t := range tlns {
		if backup.LastWriteTS.T == t.Start {
			return errors.Errorf("unable to delete: backup is a base for '%s'", t)
		}
	}

	ispitr, _, err := config.IsPITREnabled(ctx, p.Conn)
	if err != nil {
		return errors.Wrap(err, "unable check pitr state")
	}

	// if PITR is ON and there are no chunks yet we shouldn't delete the most recent back
	if !ispitr || tlns != nil {
		return nil
	}

	has, err := query.BackupHasNext(ctx, p.Conn, backup)
	if err != nil {
		return errors.Wrap(err, "check next backup")
	}
	if !has {
		return errors.New("unable to delete the last backup while PITR is on")
	}

	return nil
}

// DeleteBackupFiles removes backup's artifacts from storage
func (p *PBM) DeleteBackupFiles(meta *types.BackupMeta, stg storage.Storage) error {
	switch meta.Type {
	case defs.PhysicalBackup, defs.IncrementalBackup:
		return p.deletePhysicalBackupFiles(meta, stg)
	case defs.LogicalBackup:
		fallthrough
	default:
		var err error
		if version.IsLegacyArchive(meta.PBMVersion) {
			err = p.deleteLegacyLogicalBackupFiles(meta, stg)
		} else {
			err = p.deleteLogicalBackupFiles(meta, stg)
		}

		return err
	}
}

// DeleteBackupFiles removes backup's artifacts from storage
func (p *PBM) deletePhysicalBackupFiles(meta *types.BackupMeta, stg storage.Storage) error {
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
func (p *PBM) deleteLogicalBackupFiles(meta *types.BackupMeta, stg storage.Storage) error {
	if stg.Type() == storage.Filesystem {
		return p.deleteLogicalBackupFilesFromFS(stg, meta.Name)
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
func (p *PBM) deleteLogicalBackupFilesFromFS(stg storage.Storage, bcpName string) error {
	if err := stg.Delete(bcpName); err != nil {
		return errors.Wrapf(err, "delete %q", bcpName)
	}

	bcpMetafile := bcpName + defs.MetadataFileSuffix
	return errors.Wrapf(stg.Delete(bcpMetafile), "delete %q", bcpMetafile)
}

// deleteLegacyLogicalBackupFiles removes backup's artifacts from storage
func (p *PBM) deleteLegacyLogicalBackupFiles(meta *types.BackupMeta, stg storage.Storage) error {
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
func (p *PBM) DeleteOlderThan(ctx context.Context, t time.Time, l *log.Event) error {
	stg, err := util.GetStorage(ctx, p.Conn, l)
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	tlns, err := oplog.PITRTimelines(ctx, p.Conn)
	if err != nil {
		return errors.Wrap(err, "get PITR chunks")
	}

	cur, err := p.Conn.BcpCollection().Find(
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
		m := &types.BackupMeta{}
		err := cur.Decode(m)
		if err != nil {
			return errors.Wrap(err, "decode backup meta")
		}

		err = p.probeDelete(ctx, m, tlns)
		if err != nil {
			l.Info("deleting %s: %v", m.Name, err)
			continue
		}

		err = p.DeleteBackupFiles(m, stg)
		if err != nil {
			return errors.Wrap(err, "delete backup files from storage")
		}

		_, err = p.Conn.BcpCollection().DeleteOne(ctx, bson.M{"name": m.Name})
		if err != nil {
			return errors.Wrap(err, "delete backup meta from db")
		}
	}

	if cur.Err() != nil {
		return errors.Wrap(cur.Err(), "cursor")
	}

	return nil
}

// DeletePITR deletes backups which older than given `until` Time. It will round `until` down
// to the last write of the closest preceding backup in order not to make gaps. So usually it
// gonna leave some extra chunks. E.g. if `until = 13` and the last write of the closest preceding
// backup is `10` it will leave `11` and `12` chunks as well since `13` won't be restorable
// without `11` and `12` (contiguous timeline from the backup).
// It deletes all chunks if `until` is nil.
func (p *PBM) DeletePITR(ctx context.Context, until *time.Time, l *log.Event) error {
	stg, err := util.GetStorage(ctx, p.Conn, l)
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	var zerots primitive.Timestamp
	if until == nil {
		return p.deleteChunks(ctx, zerots, zerots, stg, l)
	}

	t := primitive.Timestamp{T: uint32(until.Unix()), I: 0}
	bcp, err := query.GetLastBackup(ctx, p.Conn, &t)
	if errors.Is(err, errors.ErrNotFound) {
		return p.deleteChunks(ctx, zerots, t, stg, l)
	}

	if err != nil {
		return errors.Wrap(err, "get recent backup")
	}

	return p.deleteChunks(ctx, zerots, bcp.LastWriteTS, stg, l)
}

func (p *PBM) deleteChunks(
	ctx context.Context,
	start, until primitive.Timestamp,
	stg storage.Storage,
	l *log.Event,
) error {
	var chunks []oplog.OplogChunk

	var err error
	if until.T > 0 {
		chunks, err = oplog.PITRGetChunksSliceUntil(ctx, p.Conn, "", until)
	} else {
		chunks, err = oplog.PITRGetChunksSlice(ctx, p.Conn, "", start, until)
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

		_, err = p.Conn.PITRChunksCollection().DeleteOne(
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
