package pbm

import (
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/version"
)

// DeleteBackup deletes backup with the given name from the current storage
// and pbm database
func (p *PBM) DeleteBackup(name string, l *log.Event) error {
	meta, err := p.GetBackupMeta(name)
	if err != nil {
		return errors.Wrap(err, "get backup meta")
	}

	tlns, err := p.PITRTimelines()
	if err != nil {
		return errors.Wrap(err, "get PITR chunks")
	}

	err = p.probeDelete(meta, tlns)
	if err != nil {
		return err
	}

	stg, err := p.GetStorage(l)
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	err = p.DeleteBackupFiles(stg, meta)
	if err != nil {
		return errors.Wrap(err, "delete files from storage")
	}

	_, err = p.Conn.Database(DB).Collection(BcpCollection).DeleteOne(p.ctx, bson.M{"name": meta.Name})
	if err != nil {
		return errors.Wrap(err, "delete metadata from db")
	}

	return nil
}

func (p *PBM) probeDelete(backup *BackupMeta, tlns []Timeline) error {
	// check if backup isn't running
	switch backup.Status {
	case StatusDone, StatusCancelled, StatusError:
	default:
		return errors.Errorf("unable to delete backup in %s state", backup.Status)
	}

	// if backup isn't a base for any PITR timeline
	for _, t := range tlns {
		if backup.LastWriteTS.T == t.Start {
			return errors.Errorf("unable to delete: backup is a base for '%s'", t)
		}
	}

	ispitr, err := p.IsPITR()
	if err != nil {
		return errors.Wrap(err, "unable check pitr state")
	}

	// if PITR is ON and there are no chunks yet we shouldn't delete the most recent back
	if !ispitr || tlns != nil {
		return nil
	}
	nxt, err := p.BackupGetNext(backup)
	if err != nil {
		return errors.Wrap(err, "check next backup")
	}
	if nxt == nil {
		return errors.New("unable to delete the last backup while PITR is on")
	}

	return nil
}

// DeleteBackupFiles removes backup's artifacts from storage
func (p *PBM) DeleteBackupFiles(stg storage.Storage, meta *BackupMeta) (err error) {
	switch meta.Type {
	case PhysicalBackup:
		return p.deletePhysicalBackupFiles(stg, meta)
	case LogicalBackup:
		fallthrough
	default:
		if version.Compatible(version.DefaultInfo.Version, meta.PBMVersion) {
			err = p.deleteLogicalBackupFiles(stg, meta)
		} else {
			err = p.deleteLegacyLogicalBackupFiles(stg, meta)
		}

		return err
	}
}

// DeleteBackupFiles removes backup's artifacts from storage
func (p *PBM) deletePhysicalBackupFiles(stg storage.Storage, meta *BackupMeta) (err error) {
	for _, r := range meta.Replsets {
		for _, f := range r.Files {
			fname := meta.Name + "/" + r.Name + "/" + f.Name + meta.Compression.Suffix()
			err = stg.Delete(fname)
			if err != nil && err != storage.ErrNotExist {
				return errors.Wrapf(err, "delete %s", fname)
			}
		}
	}

	err = stg.Delete(meta.Name + MetadataFileSuffix)
	if err == storage.ErrNotExist {
		return nil
	}

	return errors.Wrap(err, "delete metadata file from storage")
}

// deleteLogicalBackupFiles removes backup's artifacts from storage
func (p *PBM) deleteLogicalBackupFiles(stg storage.Storage, meta *BackupMeta) error {
	if meta.Store.Type == storage.Filesystem {
		return p.deleteLogicalBackupFilesFromFS(stg, meta.Name)
	}

	prefix := meta.Name + "/"
	files, err := stg.List(prefix, "")
	if err != nil {
		return errors.WithMessagef(err, "get file list: %q", prefix)
	}

	eg := errgroup.Group{}
	for _, f := range files {
		ns := prefix + f.Name
		eg.Go(func() error {
			return errors.WithMessagef(stg.Delete(ns), "delete %q", ns)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	bcpMF := meta.Name + MetadataFileSuffix
	return errors.WithMessagef(stg.Delete(bcpMF), "delete %q", bcpMF)
}

// deleteLogicalBackupFiles removes backup's artifacts from storage
func (p *PBM) deleteLogicalBackupFilesFromFS(stg storage.Storage, bcpName string) error {
	if err := stg.Delete(bcpName); err != nil {
		return errors.WithMessagef(err, "delete %q", bcpName)
	}

	bcpMetafile := bcpName + MetadataFileSuffix
	return errors.WithMessagef(stg.Delete(bcpMetafile), "delete %q", bcpMetafile)
}

// deleteLegacyLogicalBackupFiles removes backup's artifacts from storage
func (p *PBM) deleteLegacyLogicalBackupFiles(stg storage.Storage, meta *BackupMeta) (err error) {
	for _, r := range meta.Replsets {
		err = stg.Delete(r.OplogName)
		if err != nil && err != storage.ErrNotExist {
			return errors.Wrapf(err, "delete oplog %s", r.OplogName)
		}
		err = stg.Delete(r.DumpName)
		if err != nil && err != storage.ErrNotExist {
			return errors.Wrapf(err, "delete dump %s", r.DumpName)
		}
	}

	err = stg.Delete(meta.Name + MetadataFileSuffix)
	if err == storage.ErrNotExist {
		return nil
	}

	return errors.Wrap(err, "delete metadata file from storage")
}

// DeleteOlderThan deletes backups which older than given Time
func (p *PBM) DeleteOlderThan(t time.Time, l *log.Event) error {
	stg, err := p.GetStorage(l)
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	tlns, err := p.PITRTimelines()
	if err != nil {
		return errors.Wrap(err, "get PITR chunks")
	}

	cur, err := p.Conn.Database(DB).Collection(BcpCollection).Find(
		p.ctx,
		bson.M{
			"start_ts": bson.M{"$lt": t.Unix()},
		},
	)
	if err != nil {
		return errors.Wrap(err, "get backups list")
	}
	defer cur.Close(p.ctx)
	for cur.Next(p.ctx) {
		m := new(BackupMeta)
		err := cur.Decode(m)
		if err != nil {
			return errors.Wrap(err, "decode backup meta")
		}

		err = p.probeDelete(m, tlns)
		if err != nil {
			l.Info("deleting %s: %v", m.Name, err)
			continue
		}

		err = p.DeleteBackupFiles(stg, m)
		if err != nil {
			return errors.Wrap(err, "delete backup files from storage")
		}

		_, err = p.Conn.Database(DB).Collection(BcpCollection).DeleteOne(p.ctx, bson.M{"name": m.Name})
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
func (p *PBM) DeletePITR(until *time.Time, l *log.Event) error {
	stg, err := p.GetStorage(l)
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	var zerots primitive.Timestamp
	if until == nil {
		return p.deleteChunks(zerots, zerots, stg, l)
	}

	t := primitive.Timestamp{T: uint32(until.Unix()), I: 0}
	bcp, err := p.GetLastBackup(&t)
	if errors.Is(err, ErrNotFound) {
		return p.deleteChunks(zerots, t, stg, l)
	}

	if err != nil {
		return errors.Wrap(err, "get recent backup")
	}

	return p.deleteChunks(zerots, bcp.LastWriteTS, stg, l)
}

func (p *PBM) deleteChunks(start, until primitive.Timestamp, stg storage.Storage, l *log.Event) (err error) {
	var chunks []OplogChunk

	if until.T > 0 {
		chunks, err = p.PITRGetChunksSliceUntil("", until)
	} else {
		chunks, err = p.PITRGetChunksSlice("", start, until)
	}
	if err != nil {
		return errors.Wrap(err, "get pitr chunks")
	}
	if len(chunks) == 0 {
		l.Debug("nothing to delete")
	}

	for _, chnk := range chunks {
		err = stg.Delete(chnk.FName)
		if err != nil && err != storage.ErrNotExist {
			return errors.Wrapf(err, "delete pitr chunk '%s' (%v) from storage", chnk.FName, chnk)
		}

		_, err = p.Conn.Database(DB).Collection(PITRChunksCollection).DeleteOne(
			p.ctx,
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
