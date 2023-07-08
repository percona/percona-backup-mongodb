package restore

import (
	"io"
	"path"
	"strings"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
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
	mapS := pbm.MakeRSMapFunc(r.sMap)
	available, err := fetchAvailability(bcp, r.stg)
	if err != nil {
		return err
	}

	if available[databasesNS] {
		if err := r.configsvrRestoreDatabases(bcp, nss, mapRS, mapS); err != nil {
			return errors.WithMessage(err, "restore config.databases")
		}
	}

	var chunkSelector sel.ChunkSelector
	if available[collectionsNS] {
		var err error
		chunkSelector, err = r.configsvrRestoreCollections(bcp, nss, mapRS)
		if err != nil {
			return errors.WithMessage(err, "restore config.collections")
		}
	}

	if available[chunksNS] {
		if err := r.configsvrRestoreChunks(bcp, chunkSelector, mapRS, mapS); err != nil {
			return errors.WithMessage(err, "restore config.chunks")
		}
	}

	return nil
}

func fetchAvailability(bcp *pbm.BackupMeta, stg storage.Storage) (map[string]bool, error) {
	var cfgRS *pbm.BackupReplset
	for i := range bcp.Replsets {
		rs := &bcp.Replsets[i]
		if rs.IsConfigSvr != nil && *rs.IsConfigSvr {
			cfgRS = rs
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

func (r *Restore) getShardMapping(bcp *pbm.BackupMeta) map[string]string {
	source := bcp.ShardRemap
	if source == nil {
		source = make(map[string]string)
	}

	mapRevRS := pbm.MakeReverseRSMapFunc(r.rsMap)
	rv := make(map[string]string)
	for _, s := range r.shards {
		sourceRS := mapRevRS(s.RS)
		sourceS := source[sourceRS]
		if sourceS == "" {
			sourceS = sourceRS
		}
		if sourceS != s.ID {
			rv[sourceS] = s.ID
		}
	}

	return rv
}

// configsvrRestoreDatabases upserts config.databases documents
// for selected databases
func (r *Restore) configsvrRestoreDatabases(bcp *pbm.BackupMeta, nss []string, mapRS, mapS pbm.RSMapFunc) error {
	filepath := path.Join(bcp.Name, mapRS(r.node.RS()), "config.databases"+bcp.Compression.Suffix())
	rdr, err := r.stg.SourceReader(filepath)
	if err != nil {
		return err
	}
	rdr, err = compress.Decompress(rdr, bcp.Compression)
	if err != nil {
		return err
	}

	allowedDBs := make(map[string]bool)
	for _, ns := range nss {
		db, _, _ := strings.Cut(ns, ".")
		allowedDBs[db] = true
	}

	// go config.databases' docs and pick only selected databases docs
	// insert/replace in bulk
	models := []mongo.WriteModel{}
	buf := make([]byte, archive.MaxBSONSize)
	for {
		buf, err = archive.ReadBSONBuffer(rdr, buf[:cap(buf)])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return err
		}

		db := bson.Raw(buf).Lookup("_id").StringValue()
		if !allowedDBs[db] {
			continue
		}

		doc := bson.D{}
		if err = bson.Unmarshal(buf, &doc); err != nil {
			return errors.WithMessage(err, "unmarshal")
		}

		for i, a := range doc {
			if a.Key == "primary" {
				doc[i].Value = mapS(doc[i].Value.(string))
				break
			}
		}

		model := mongo.NewReplaceOneModel()
		model.SetFilter(bson.D{{"_id", db}})
		model.SetReplacement(doc)
		model.SetUpsert(true)
		models = append(models, model)
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
func (r *Restore) configsvrRestoreCollections(bcp *pbm.BackupMeta, nss []string, mapRS pbm.RSMapFunc) (sel.ChunkSelector, error) {
	ver, err := pbm.GetMongoVersion(r.cn.Context(), r.node.Session())
	if err != nil {
		return nil, errors.WithMessage(err, "get mongo version")
	}

	var chunkSelector sel.ChunkSelector
	if ver.Major() >= 5 {
		chunkSelector = sel.NewUUIDChunkSelector()
	} else {
		chunkSelector = sel.NewNSChunkSelector()
	}

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

	models := []mongo.WriteModel{}
	buf := make([]byte, archive.MaxBSONSize)
	for {
		buf, err = archive.ReadBSONBuffer(rdr, buf[:cap(buf)])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		ns := bson.Raw(buf).Lookup("_id").StringValue()
		if !selected(ns) {
			continue
		}

		chunkSelector.Add(bson.Raw(buf))

		doc := bson.D{}
		err = bson.Unmarshal(buf, &doc)
		if err != nil {
			return nil, errors.WithMessage(err, "unmarshal")
		}

		model := mongo.NewReplaceOneModel()
		model.SetFilter(bson.D{{"_id", ns}})
		model.SetReplacement(doc)
		model.SetUpsert(true)
		models = append(models, model)
	}

	if len(models) == 0 {
		return chunkSelector, nil
	}

	coll := r.cn.Conn.Database("config").Collection("collections")
	if _, err = coll.BulkWrite(r.cn.Context(), models); err != nil {
		return nil, errors.WithMessage(err, "update config.collections")
	}

	return chunkSelector, nil
}

// configsvrRestoreChunks upserts config.chunks documents for selected namespaces
func (r *Restore) configsvrRestoreChunks(bcp *pbm.BackupMeta, selector sel.ChunkSelector, mapRS, mapS pbm.RSMapFunc) error {
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
	_, err = coll.DeleteMany(r.cn.Context(), selector.BuildFilter())
	if err != nil {
		return err
	}

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

			if !selector.Selected(bson.Raw(buf)) {
				continue
			}

			doc := bson.D{}
			if err := bson.Unmarshal(buf, &doc); err != nil {
				return errors.WithMessage(err, "unmarshal")
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

		if len(models) == 0 {
			return nil
		}

		_, err = coll.BulkWrite(r.cn.Context(), models)
		if err != nil {
			return errors.WithMessage(err, "update config.chunks")
		}

		models = models[:0]
	}

	return nil
}
