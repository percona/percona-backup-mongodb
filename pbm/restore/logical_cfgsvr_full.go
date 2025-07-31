package restore

import (
	"context"
	"encoding/hex"
	"io"
	"path"

	"github.com/percona/percona-backup-mongodb/pbm/archive"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// configsvrFullRestore restores config.collections and config.chunks
// for the purpose of full logical restore.
// It adds special handling for config.system.sessions routing data.
func (r *Restore) configsvrFullRestore(
	ctx context.Context,
	bcp *backup.BackupMeta,
	mapRS util.RSMapFunc,
) error {
	mapS := util.MakeRSMapFunc(r.sMap)

	bcpSysSessUUIDToSkip, err := r.fullRestoreConfigCollections(ctx, bcp, mapRS)
	if err != nil {
		return errors.Wrap(err, "full restore config.collections")
	}

	if err := r.fullRestoreConfigChunks(ctx, bcp, bcpSysSessUUIDToSkip, mapRS, mapS); err != nil {
		return errors.Wrap(err, "full restore config.chunks")
	}

	return nil
}

// fullRestoreConfigCollections does full restore of config.collections
// collection. It adds special handling for system.sessions document within
// collection:
//   - on target cluster it just leave that document as is
//   - in case of backup data it just ignores possible entry within collection dump
//   - for the backup data it finds UUID for config.system.sessions collection referneced
//     within the backup data. By having that UUID it'll be possible to filter out
//     corresponding chunk's documents late in the processing logic.
//
// fullRestoreConfigCollections returns UUID for the config.system.session collection
// referenced within the backup. In case of error mentioned UUID is empty string.
func (r *Restore) fullRestoreConfigCollections(
	ctx context.Context,
	bcp *backup.BackupMeta,
	mapRS util.RSMapFunc,
) (string, error) {
	filepath := path.Join(bcp.Name, mapRS(r.brief.SetName), defs.ConfigCollectionsNS+bcp.Compression.Suffix())
	rdr, err := r.bcpStg.SourceReader(filepath)
	if err != nil {
		return "", err
	}
	defer rdr.Close()

	rdr, err = compress.Decompress(rdr, bcp.Compression)
	if err != nil {
		return "", err
	}

	if err = r.cleanUpConfigCollections(ctx); err != nil {
		return "", errors.Wrap(err, "cleaning up config.collections")
	}
	r.log.Debug("cleaned up config.collections before restoring")

	bcpSysSessUUID := ""
	models := []mongo.WriteModel{}
	buf := make([]byte, archive.MaxBSONSize)
	for {
		buf, err = archive.ReadBSONBuffer(rdr, buf[:cap(buf)])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return "", err
		}

		bsonDoc := bson.Raw(buf)
		ns := bsonDoc.Lookup("_id").StringValue()
		if ns == defs.ConfigSystemSessionsNS {
			_, uuid, ok := bsonDoc.Lookup("uuid").BinaryOK()
			if ok {
				// save confgi.system.sessions UUID, we need it for filtering out chunks docs
				bcpSysSessUUID = hex.EncodeToString(uuid)
			}

			// skip system.sessions if it's in the backup
			continue
		}

		doc := bson.D{}
		err = bson.Unmarshal(buf, &doc)
		if err != nil {
			return "", errors.Wrap(err, "unmarshal")
		}

		model := mongo.NewReplaceOneModel()
		model.SetFilter(bson.D{{"_id", ns}})
		model.SetReplacement(doc)
		model.SetUpsert(true)
		models = append(models, model)
	}

	if len(models) == 0 {
		return bcpSysSessUUID, nil
	}

	coll := r.leadConn.ConfigDatabase().Collection("collections")
	res, err := coll.BulkWrite(ctx, models)
	if err != nil {
		return "", errors.Wrap(err, "restore config.collections")
	}
	r.log.Debug("finished restoring config.collections (%d documents)", res.InsertedCount)

	return bcpSysSessUUID, nil
}

// fullRestoreConfigChunks does full restore of config.chunks collection.
// It adds special handling for system.sessions related documents within
// collection:
//   - on the target cluster it just leaves all related docs untouched.
//   - it uses sysSessToSkip parameter which represents UUID for the system.sessions
//     collection, to be able to skip referenced chunks within the backup dump.
//
// All other chunk docs are deleted before PBM start to restore docs from the backup.
func (r *Restore) fullRestoreConfigChunks(
	ctx context.Context,
	bcp *backup.BackupMeta,
	sysSessToSkip string,
	mapRS,
	mapS util.RSMapFunc,
) error {
	filepath := path.Join(bcp.Name, mapRS(r.brief.SetName), defs.ConfigChunksNS+bcp.Compression.Suffix())
	rdr, err := r.bcpStg.SourceReader(filepath)
	if err != nil {
		return err
	}
	defer rdr.Close()

	rdr, err = compress.Decompress(rdr, bcp.Compression)
	if err != nil {
		return err
	}

	if err = r.cleanUpConfigChunks(ctx); err != nil {
		return errors.Wrap(err, "clean up config.chunks during full restore")
	}
	r.log.Debug("cleaned up config.chunks before restoring")

	var docInserted int64
	models := []mongo.WriteModel{}
	buf := make([]byte, archive.MaxBSONSize)
	for done := false; !done; {
		// there could be thousands of chunks. write every maxBulkWriteCount docs
		// to limit memory usage
		for i := 0; i != maxBulkWriteCount; i++ {
			buf, err = archive.ReadBSONBuffer(rdr, buf[:cap(buf)])
			if err != nil {
				if errors.Is(err, io.EOF) {
					done = true
					break
				}

				return err
			}

			if len(sysSessToSkip) != 0 {
				// we need to skip chunks that reference system.sessions
				bsonDoc := bson.Raw(buf)
				if _, uuid, ok := bsonDoc.Lookup("uuid").BinaryOK(); ok {
					if hex.EncodeToString(uuid) == sysSessToSkip {
						continue
					}
				}
			}

			doc := bson.D{}
			if err := bson.Unmarshal(buf, &doc); err != nil {
				return errors.Wrap(err, "unmarshal")
			}

			for i, a := range doc {
				switch a.Key {
				case "shard":
					doc[i].Value = mapS(doc[i].Value.(string))
				case "history":
					history := doc[i].Value.(bson.A)
					for j, b := range history {
						c := b.(bson.D)
						for k, d := range c {
							if d.Key == "shard" {
								c[k].Value = mapS(d.Value.(string))
							}
						}
						history[j] = c
					}
					doc[i].Value = history
				}
			}

			models = append(models, mongo.NewInsertOneModel().SetDocument(doc))
		}

		if len(models) == 0 && !done {
			// if it's not done, we just reached maxBulkWriteCount, we need to process more
			continue
		} else if len(models) == 0 && done {
			// it's done and there's nothing to update
			return nil
		}

		res, err := r.leadConn.ConfigDatabase().Collection("chunks").BulkWrite(ctx, models)
		if err != nil {
			return errors.Wrap(err, "restore config.chunks")
		}
		docInserted += res.InsertedCount

		models = models[:0]
	}
	r.log.Debug("finished restoring config.chunks (%d documents)", docInserted)

	return nil
}

// cleanUpConfigCollections deletes complete config.collections collection except
// config.system.sessions related document.
func (r *Restore) cleanUpConfigCollections(ctx context.Context) error {
	excSessFilter := bson.M{
		"_id": bson.M{"$ne": defs.ConfigSystemSessionsNS},
	}
	_, err := r.leadConn.ConfigDatabase().Collection("collections").DeleteMany(ctx, excSessFilter)
	if err != nil {
		return errors.Wrap(err, "delete all from config.collections")
	}

	return nil
}

// cleanUpConfigChunks deletes complete config.chunks collection except
// config.system.sessions releted documents.
// Before performing deletion, it finds out system.sessions UUID by querying
// config.collections.
func (r *Restore) cleanUpConfigChunks(ctx context.Context) error {
	sessBson, err := r.leadConn.ConfigDatabase().Collection("collections").
		FindOne(ctx, bson.D{{"_id", defs.ConfigSystemSessionsNS}}).Raw()
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return errors.Wrap(err, "querying config.system.sessions UUID")
	}

	excSessFilter := bson.M{}
	if sessBson != nil {
		if subtype, uuid, ok := sessBson.Lookup("uuid").BinaryOK(); ok {
			uuid := primitive.Binary{Subtype: subtype, Data: uuid}
			excSessFilter["uuid"] = bson.M{"$ne": uuid}
		}
	}

	_, err = r.leadConn.ConfigDatabase().Collection("chunks").
		DeleteMany(ctx, excSessFilter)
	if err != nil {
		return errors.Wrap(err, "delete all from config.chunks")
	}

	return nil
}
