package resync

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/internal/backup"
	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/oplog"
	"github.com/percona/percona-backup-mongodb/internal/restore"
	"github.com/percona/percona-backup-mongodb/internal/storage"
	"github.com/percona/percona-backup-mongodb/internal/util"
	"github.com/percona/percona-backup-mongodb/internal/version"
)

// ResyncStorage updates PBM metadata (snapshots and pitr) according to the data in the storage
func ResyncStorage(ctx context.Context, m connect.Client, l log.LogEvent) error {
	stg, err := util.GetStorage(ctx, m, l)
	if err != nil {
		return errors.Wrap(err, "unable to get backup store")
	}

	_, err = stg.FileStat(defs.StorInitFile)
	if errors.Is(err, storage.ErrNotExist) {
		err = stg.Save(defs.StorInitFile, bytes.NewBufferString(version.Current().Version), 0)
	}
	if err != nil {
		return errors.Wrap(err, "init storage")
	}

	rstrs, err := stg.List(defs.PhysRestoresDir, ".json")
	if err != nil {
		return errors.Wrap(err, "get physical restores list from the storage")
	}
	l.Debug("got physical restores list: %v", len(rstrs))
	for _, rs := range rstrs {
		rname := strings.TrimSuffix(rs.Name, ".json")
		rmeta, err := restore.GetPhysRestoreMeta(rname, stg, l)
		if err != nil {
			l.Error("get meta for restore %s: %v", rs.Name, err)
			if rmeta == nil {
				continue
			}
		}

		_, err = m.RestoresCollection().ReplaceOne(
			ctx,
			bson.D{{"name", rmeta.Name}},
			rmeta,
			options.Replace().SetUpsert(true),
		)
		if err != nil {
			return errors.Wrapf(err, "upsert restore %s/%s", rmeta.Name, rmeta.Backup)
		}
	}

	bcps, err := stg.List("", defs.MetadataFileSuffix)
	if err != nil {
		return errors.Wrap(err, "get a backups list from the storage")
	}
	l.Debug("got backups list: %v", len(bcps))

	_, err = m.BcpCollection().DeleteMany(ctx, bson.M{})
	if err != nil {
		return errors.Wrapf(err, "clean up %s", defs.BcpCollection)
	}

	_, err = m.PITRChunksCollection().DeleteMany(ctx, bson.M{})
	if err != nil {
		return errors.Wrapf(err, "clean up %s", defs.PITRChunksCollection)
	}

	var ins []interface{}
	for _, b := range bcps {
		l.Debug("bcp: %v", b.Name)

		d, err := stg.SourceReader(b.Name)
		if err != nil {
			return errors.Wrapf(err, "read meta for %v", b.Name)
		}

		v := backup.BackupMeta{}
		err = json.NewDecoder(d).Decode(&v)
		d.Close()
		if err != nil {
			return errors.Wrapf(err, "unmarshal backup meta [%s]", b.Name)
		}
		err = backup.CheckBackupFiles(ctx, &v, stg)
		if err != nil {
			l.Warning("skip snapshot %s: %v", v.Name, err)
			v.Status = defs.StatusError
			v.Err = err.Error()
		}
		ins = append(ins, v)
	}

	if len(ins) != 0 {
		_, err = m.BcpCollection().InsertMany(ctx, ins)
		if err != nil {
			return errors.Wrap(err, "insert retrieved backups meta")
		}
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
		chnk := oplog.PITRmetaFromFName(f.Name)
		if chnk != nil {
			chnk.Size = stat.Size
			pitr = append(pitr, chnk)
		}
	}

	if len(pitr) == 0 {
		return nil
	}

	_, err = m.PITRChunksCollection().InsertMany(ctx, pitr)
	if err != nil {
		return errors.Wrap(err, "insert retrieved pitr meta")
	}

	return nil
}
