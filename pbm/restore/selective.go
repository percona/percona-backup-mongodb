package restore

import (
	"io"
	"path"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/archive"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/sel"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

const (
	databasesNS   = "databases"
	collectionsNS = "collections"
	chunksNS      = "chunks"
)

const maxBulkWriteCount = 500

// configsvrRestore restores for selected namespaces
func (r *Restore) configsvrRestore(bcp *pbm.BackupMeta, nss []string, mapRS pbm.RSMapFunc) error {
	available, err := fetchAvailability(bcp, r.stg)
	if err != nil {
		return err
	}

	if available[databasesNS] {
		if err := r.configsvrRestoreDatabases(bcp, nss, mapRS); err != nil {
			return errors.WithMessage(err, "restore config.databases")
		}
	}

	var uuids []primitive.Binary
	if available[collectionsNS] {
		var err error
		uuids, err = r.configsvrRestoreCollections(bcp, nss, mapRS)
		if err != nil {
			return errors.WithMessage(err, "restore config.collections")
		}
	}

	if available[chunksNS] {
		if err := r.configsvrRestoreChunks(bcp, uuids, mapRS); err != nil {
			return errors.WithMessage(err, "restore config.chunks")
		}
	}

	return nil
}

func fetchAvailability(bcp *pbm.BackupMeta, stg storage.Storage) (map[string]bool, error) {
	var cfgRS *pbm.BackupReplset
	for _, rs := range bcp.Replsets {
		if rs.IsConfigSvr != nil && *rs.IsConfigSvr {
			cfgRS = &rs
			break
		}
	}
	if cfgRS == nil {
		return nil, errors.New("no configsvr replset metadata found")
	}

	nss, err := pbm.ReadArchiveNamespaces(stg, cfgRS.DumpName)
	if err != nil {
		return nil, errors.WithMessagef(err, "read archive namespaces %q", cfgRS.DumpName)
	}

	rv := make(map[string]bool)
	for _, ns := range nss {
		switch ns.Collection {
		case databasesNS, collectionsNS, chunksNS:
			rv[ns.Collection] = ns.Size != 0
		}
	}

	return rv, nil
}

// configsvrRestoreDatabases upserts config.databases documents
// for selected databases
func (r *Restore) configsvrRestoreDatabases(bcp *pbm.BackupMeta, nss []string, mapRS pbm.RSMapFunc) error {
	filepath := path.Join(bcp.Name, mapRS(r.node.RS()), "config.databases"+bcp.Compression.Suffix())
	rdr, err := r.stg.SourceReader(filepath)
	if err != nil {
		return err
	}
	rdr, err = compress.Decompress(rdr, bcp.Compression)
	if err != nil {
		return err
	}

	selected := sel.MakeSelectedPred(nss)

	// go config.databases' docs and pick only selected databases docs
	// insert/replace in bulk
	models := []mongo.WriteModel{}
	for buf := make([]byte, archive.MaxBSONSize); ; {
		var raw []byte

		raw, err = archive.ReadBSONBuffer(rdr, buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return err
		}

		db := bson.Raw(raw).Lookup("_id").StringValue()
		if !selected(db) {
			continue
		}

		doc := bson.D{}
		if err = bson.Unmarshal(raw, &doc); err != nil {
			return errors.WithMessage(err, "unmarshal")
		}

		for i, a := range doc {
			if a.Key == "primaryShard" {
				doc[i].Value = mapRS(doc[i].Value.(string))
				break
			}
		}

		model := mongo.NewReplaceOneModel()
		model.SetFilter(bson.D{{"_id", db}})
		model.SetReplacement(doc)
		model.SetUpsert(true)
		models = append(models, model)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if len(models) == 0 {
		return nil
	}

	coll := r.cn.Conn.Database("config").Collection("databases")
	_, err = coll.BulkWrite(r.cn.Context(), models)
	return errors.WithMessage(err, "update config.databases")
}

// configsvrRestoreCollections upserts config.collections documents
// for selected namespaces
func (r *Restore) configsvrRestoreCollections(bcp *pbm.BackupMeta, nss []string, mapRS pbm.RSMapFunc) ([]primitive.Binary, error) {
	filepath := path.Join(bcp.Name, mapRS(r.node.RS()), "config.collections"+bcp.Compression.Suffix())
	rdr, err := r.stg.SourceReader(filepath)
	if err != nil {
		return nil, err
	}
	rdr, err = compress.Decompress(rdr, bcp.Compression)
	if err != nil {
		return nil, err
	}

	selected := sel.MakeSelectedPred(nss)

	uuids := []primitive.Binary{}
	models := []mongo.WriteModel{}
	for buf := make([]byte, archive.MaxBSONSize); ; {
		var raw []byte

		raw, err := archive.ReadBSONBuffer(rdr, buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		ns := bson.Raw(raw).Lookup("_id").StringValue()
		if !selected(ns) {
			continue
		}

		subtype, data := bson.Raw(raw).Lookup("uuid").Binary()
		uuids = append(uuids, primitive.Binary{Subtype: subtype, Data: data})

		doc := bson.D{}
		if err = bson.Unmarshal(raw, &doc); err != nil {
			return nil, errors.WithMessage(err, "unmarshal")
		}

		model := mongo.NewReplaceOneModel()
		model.SetFilter(bson.D{{"_id", ns}})
		model.SetReplacement(doc)
		model.SetUpsert(true)
		models = append(models, model)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if len(models) == 0 {
		return uuids, nil
	}

	coll := r.cn.Conn.Database("config").Collection("collections")
	if _, err = coll.BulkWrite(r.cn.Context(), models); err != nil {
		return nil, errors.WithMessage(err, "update config.collections")
	}

	return uuids, nil
}

// configsvrRestoreChunks upserts config.chunks documents for selected namespaces
func (r *Restore) configsvrRestoreChunks(bcp *pbm.BackupMeta, uuids []primitive.Binary, mapRS pbm.RSMapFunc) error {
	filepath := path.Join(bcp.Name, mapRS(r.node.RS()), "config.chunks"+bcp.Compression.Suffix())
	rdr, err := r.stg.SourceReader(filepath)
	if err != nil {
		return err
	}
	rdr, err = compress.Decompress(rdr, bcp.Compression)
	if err != nil {
		return err
	}

	coll := r.cn.Conn.Database("config").Collection("chunks")
	_, err = coll.DeleteMany(r.cn.Context(), bson.D{{"uuid", bson.M{"$in": uuids}}})
	if err != nil {
		return err
	}

	selected := func(uuid primitive.Binary) bool {
		for _, u := range uuids {
			if uuid.Equal(u) {
				return true
			}
		}

		return false
	}

	models := []mongo.WriteModel{}
	buf := make([]byte, archive.MaxBSONSize)
	for done := false; done; {
		// there could be thousands of chunks. write every maxBulkWriteCount docs
		// to limit memory usage
		for i := 0; i != maxBulkWriteCount; i++ {
			var raw []byte

			raw, err = archive.ReadBSONBuffer(rdr, buf)
			if err != nil {
				break
			}

			subtype, raw := bson.Raw(raw).Lookup("uuid").Binary()
			if !selected(primitive.Binary{Subtype: subtype, Data: raw}) {
				continue
			}

			doc := bson.D{}
			if err := bson.Unmarshal(raw, &doc); err != nil {
				return errors.WithMessage(err, "unmarshal")
			}

			for i, a := range doc {
				switch a.Key {
				case "shard":
					doc[i].Value = mapRS(doc[i].Value.(string))
				case "history":
					history := doc[i].Value.(bson.D)
					for j, b := range history {
						if b.Key == "shard" {
							history[j].Value = mapRS(history[j].Value.(string))
						}
					}
				}
			}

			models = append(models, mongo.NewInsertOneModel().SetDocument(doc))
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}

			done = true
		}
		if len(models) == 0 {
			return nil
		}

		_, err = coll.BulkWrite(r.cn.Context(), models)
		if err != nil {
			return errors.WithMessage(err, "update config.chunks")
		}

		models = models[:0]
	}

	return err
}
