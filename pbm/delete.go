package pbm

import (
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
)

// DeleteBackup deletes backup with the given name from the current storage
// and pbm database
func (p *PBM) DeleteBackup(name string) error {
	meta, err := p.GetBackupMeta(name)
	if err != nil {
		return errors.Wrap(err, "get backup meta")
	}

	stg, err := p.GetStorage()
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	return p.deleteBackup(meta, stg)
}

func (p *PBM) deleteBackup(meta *BackupMeta, stg storage.Storage) (err error) {
	switch meta.Status {
	case StatusDone, StatusError:
	default:
		return errors.Errorf("Unable to delete backup in %s state", meta.Status)
	}
	for _, r := range meta.Replsets {
		err = stg.Delete(r.OplogName)
		if err != nil {
			return errors.Wrapf(err, "delete oplog %s", r.OplogName)
		}
		err = stg.Delete(r.DumpName)
		if err != nil {
			return errors.Wrapf(err, "delete dump %s", r.OplogName)
		}
	}

	err = stg.Delete(meta.Name + MetadataFileSuffix)
	if err != nil {
		return errors.Wrap(err, "delete metadata file from storage")
	}

	_, err = p.Conn.Database(DB).Collection(BcpCollection).DeleteOne(p.ctx, bson.M{"name": meta.Name})
	return errors.Wrap(err, "delete metadata from db")
}

// DeleteOlderThan deletes backups which older than backup
// with the given name from the current storage and pbm database
func (p *PBM) DeleteOlderThan(name string) error {
	meta, err := p.GetBackupMeta(name)
	if err != nil {
		return errors.Wrap(err, "get backup meta")
	}

	stg, err := p.GetStorage()
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	cur, err := p.Conn.Database(DB).Collection(BcpCollection).Find(
		p.ctx,
		bson.M{
			"start_ts": bson.M{"$lt": meta.StartTS},
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

		err = p.deleteBackup(m, stg)
		if err != nil {
			return errors.Wrap(err, "delete backup")
		}
	}

	return errors.Wrap(cur.Err(), "cursor")
}
