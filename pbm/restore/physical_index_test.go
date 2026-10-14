package restore

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/mongodb/mongo-tools/common/db"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/restore/phys"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/storage/fs"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/version"
)

const (
	idIndexName        = "_id_"
	testOplogChunkName = "rs0/test.oplog"
)

// replay timeframe of the physical PITR. All the test ops belong to it.
var (
	replayFrom = ts(100)
	replayTo   = ts(200)
)

func TestReplayOplogWithIndexes(t *testing.T) {
	t.Run("index is created within oplog", func(t *testing.T) {
		coll := setupCollection(t, "phys_idx_create", "c1")

		replayIndexOplog(
			t,
			createIndexesOp(t, coll, ts(110), "fieldX_-1", bson.D{{"fieldX", -1}}),
		)

		assertIndexNames(t, coll, idIndexName, "fieldX_-1")
	})

	t.Run("index is dropped within oplog", func(t *testing.T) {
		// the index is a part of the data files restored from the backup
		coll := setupCollection(
			t,
			"phys_idx_drop",
			"c1",
			mongo.IndexModel{
				Keys:    bson.D{{"fieldX", -1}},
				Options: options.Index().SetName("fieldX_-1"),
			},
		)

		replayIndexOplog(
			t,
			dropIndexesOp(t, coll, ts(110), "fieldX_-1"),
		)

		assertIndexNames(t, coll, idIndexName)
	})

	t.Run("index is created and dropped within oplog", func(t *testing.T) {
		coll := setupCollection(t, "phys_idx_create_drop", "c1")

		replayIndexOplog(
			t,
			createIndexesOp(t, coll, ts(110), "fieldX_-1", bson.D{{"fieldX", -1}}),
			dropIndexesOp(t, coll, ts(120), "fieldX_-1"),
		)

		assertIndexNames(t, coll, idIndexName)
	})

	t.Run("several indexes are created and some of them dropped within oplog", func(t *testing.T) {
		coll := setupCollection(t, "phys_idx_create_drop_many", "c1")

		replayIndexOplog(
			t,
			createIndexesOp(t, coll, ts(110), "fieldA_1", bson.D{{"fieldA", 1}}),
			createIndexesOp(t, coll, ts(111), "fieldB_1", bson.D{{"fieldB", 1}}),
			createIndexesOp(t, coll, ts(112), "fieldC_-1", bson.D{{"fieldC", -1}}),
			createIndexesOp(t, coll, ts(113), "fieldD_-1", bson.D{{"fieldD", -1}}),
			dropIndexesOp(t, coll, ts(120), "fieldB_1"),
			dropIndexesOp(t, coll, ts(121), "fieldD_-1"),
		)

		assertIndexNames(t, coll, idIndexName, "fieldA_1", "fieldC_-1")
	})

	t.Run("existing and oplog created indexes are dropped within oplog", func(t *testing.T) {
		coll := setupCollection(
			t,
			"phys_idx_pre_create_drop",
			"c1",
			mongo.IndexModel{
				Keys:    bson.D{{"preA", 1}},
				Options: options.Index().SetName("preA_1"),
			},
			mongo.IndexModel{
				Keys:    bson.D{{"preB", -1}},
				Options: options.Index().SetName("preB_-1"),
			},
		)

		replayIndexOplog(
			t,
			createIndexesOp(t, coll, ts(110), "fieldA_1", bson.D{{"fieldA", 1}}),
			createIndexesOp(t, coll, ts(111), "fieldB_1", bson.D{{"fieldB", 1}}),
			createIndexesOp(t, coll, ts(112), "fieldC_-1", bson.D{{"fieldC", -1}}),
			createIndexesOp(t, coll, ts(113), "fieldD_-1", bson.D{{"fieldD", -1}}),
			dropIndexesOp(t, coll, ts(120), "fieldB_1"),
			dropIndexesOp(t, coll, ts(121), "fieldD_-1"),
			dropIndexesOp(t, coll, ts(122), "preB_-1"),
		)

		assertIndexNames(t, coll, idIndexName, "preA_1", "fieldA_1", "fieldC_-1")
	})

	t.Run("existing TTL index is modified within oplog", func(t *testing.T) {
		// the TTL index is a part of the data files restored from the backup,
		// so it's not in the index catalog and collMod is applied on the node
		coll := setupCollection(
			t,
			"phys_idx_pre_collmod",
			"c1",
			mongo.IndexModel{
				Keys:    bson.D{{"createdAt", 1}},
				Options: options.Index().SetName("ttl_1").SetExpireAfterSeconds(100),
			},
		)

		replayIndexOplog(
			t,
			collModOp(t, coll, ts(110), "ttl_1", bson.D{
				{"expireAfterSeconds", 200},
				{"hidden", true},
			}),
		)

		assertIndexNames(t, coll, idIndexName, "ttl_1")
		assertIndexProperties(t, coll, "ttl_1", bson.M{"expireAfterSeconds": 200, "hidden": true})
	})

	t.Run("existing index is hidden within oplog", func(t *testing.T) {
		// collMod is not limited to the TTL indexes
		coll := setupCollection(
			t,
			"phys_idx_pre_collmod_hidden",
			"c1",
			mongo.IndexModel{
				Keys:    bson.D{{"fieldX", -1}},
				Options: options.Index().SetName("fieldX_-1"),
			},
		)

		replayIndexOplog(
			t,
			collModOp(t, coll, ts(110), "fieldX_-1", bson.D{{"hidden", true}}),
		)

		assertIndexNames(t, coll, idIndexName, "fieldX_-1")
		assertIndexProperties(t, coll, "fieldX_-1", bson.M{"hidden": true})
	})

	t.Run("index created within oplog is modified within oplog", func(t *testing.T) {
		coll := setupCollection(t, "phys_idx_created_collmod", "c1")

		// the index is in the catalog, so collMod is applied on the catalog
		// and the index is built with the modified option
		replayIndexOplog(
			t,
			createTTLIndexesOp(t, coll, ts(110), "ttl_1", 100),
			collModOp(t, coll, ts(120), "ttl_1", bson.D{
				{"expireAfterSeconds", 200},
				{"hidden", true},
			}),
		)

		assertIndexNames(t, coll, idIndexName, "ttl_1")
		assertIndexProperties(t, coll, "ttl_1", bson.M{"expireAfterSeconds": 200, "hidden": true})
	})
}

// replayIndexOplog replays ops on the test mongod the same way
// the physical PITR does it on the standalone node.
func replayIndexOplog(t *testing.T, ops ...db.Oplog) {
	t.Helper()

	oplogRanges := []oplogRange{{
		chunks: []oplog.OplogChunk{{
			RS:          "rs0",
			FName:       testOplogChunkName,
			Compression: compress.CompressionTypeNone,
			StartTS:     replayFrom,
			EndTS:       replayTo,
		}},
		storage: saveOplogChunk(t, ops),
	}}

	ctx := t.Context()
	mClient := leadConn.MongoClient()
	mgoV, err := version.GetMongoVersion(ctx, mClient)
	if err != nil {
		t.Fatalf("get mongo version: %v", err)
	}

	r := &PhysRestore{nodeInfo: &topo.NodeInfo{}, log: log.DiscardEvent}

	oplogOption := applyOplogOption{start: &replayFrom, end: &replayTo, unsafe: true}
	stat := phys.DistTxnStat{}
	_, err = r.replayOplogWithIndexes(ctx, mClient, oplogRanges, &oplogOption, &stat, &mgoV)
	if err != nil {
		t.Fatalf("replay oplog with indexes: %v", err)
	}
}

// setupCollection creates the collection with a single document and the
// specified indexes.
func setupCollection(
	t *testing.T,
	dbName, collName string,
	idxs ...mongo.IndexModel,
) *mongo.Collection {
	t.Helper()

	ctx := t.Context()
	m := leadConn.MongoClient()
	if err := m.Database(dbName).Drop(ctx); err != nil {
		t.Fatalf("drop test db %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		// t.Context() is already canceled when the cleanup runs
		if err := m.Database(dbName).Drop(context.Background()); err != nil {
			t.Logf("cleanup: drop test db %s: %v", dbName, err)
		}
	})

	coll := m.Database(dbName).Collection(collName)
	if _, err := coll.InsertOne(ctx, bson.D{{"fieldX", 1}}); err != nil {
		t.Fatalf("insert doc: %v", err)
	}

	if len(idxs) > 0 {
		if _, err := coll.Indexes().CreateMany(ctx, idxs); err != nil {
			t.Fatalf("create existing indexes: %v", err)
		}
	}

	return coll
}

// saveOplogChunk saves ops as a single uncompressed oplog chunk
// and returns the storage which holds it.
func saveOplogChunk(t *testing.T, ops []db.Oplog) storage.Storage {
	t.Helper()

	stg, err := fs.New(&fs.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("create fs storage: %v", err)
	}

	buf := &bytes.Buffer{}
	for _, op := range ops {
		raw, err := bson.Marshal(op)
		if err != nil {
			t.Fatalf("marshal oplog op: %v", err)
		}
		if _, err := buf.Write(raw); err != nil {
			t.Fatalf("write oplog op: %v", err)
		}
	}

	if err := stg.Save(testOplogChunkName, bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("save oplog chunk: %v", err)
	}

	return stg
}

// assertIndexNames checks that the collection has exactly the wanted indexes.
func assertIndexNames(t *testing.T, coll *mongo.Collection, want ...string) {
	t.Helper()

	ctx := t.Context()
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}

	var specs []struct {
		Name string `bson:"name"`
	}
	if err := cur.All(ctx, &specs); err != nil {
		t.Fatalf("decode indexes: %v", err)
	}

	got := make([]string, 0, len(specs))
	for _, spec := range specs {
		got = append(got, spec.Name)
	}

	slices.Sort(got)
	slices.Sort(want)

	if !slices.Equal(want, got) {
		t.Errorf("wrong indexes for %s: want=%v, got=%v", coll.Name(), want, got)
	}
}

// assertIndexProperties checks options of the collection's index.
func assertIndexProperties(t *testing.T, coll *mongo.Collection, idxName string, want bson.M) {
	t.Helper()

	ctx := t.Context()
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}

	var specs []bson.M
	if err := cur.All(ctx, &specs); err != nil {
		t.Fatalf("decode indexes: %v", err)
	}

	for _, spec := range specs {
		if spec["name"] != idxName {
			continue
		}
		for opt, wantVal := range want {
			gotVal, ok := spec[opt]
			if !ok {
				t.Errorf("index %s has no %q option", idxName, opt)
				continue
			}
			if fmt.Sprint(gotVal) != fmt.Sprint(wantVal) {
				t.Errorf("wrong %q for index %s: want=%v, got=%v", opt, idxName, wantVal, gotVal)
			}
		}
		return
	}

	t.Fatalf("index %s not found", idxName)
}

func createIndexesOp(
	t *testing.T,
	coll *mongo.Collection,
	ts bson.Timestamp,
	idxName string,
	key bson.D,
) db.Oplog {
	t.Helper()

	return cmdOp(t, coll, ts, bson.D{
		{"createIndexes", coll.Name()},
		{"v", 2},
		{"key", key},
		{"name", idxName},
	})
}

func dropIndexesOp(
	t *testing.T,
	coll *mongo.Collection,
	ts bson.Timestamp,
	idxName string,
) db.Oplog {
	t.Helper()

	return cmdOp(t, coll, ts, bson.D{
		{"dropIndexes", coll.Name()},
		{"index", idxName},
	})
}

// createTTLIndexesOp creates "createIndexes" op for a TTL index on "createdAt".
func createTTLIndexesOp(
	t *testing.T,
	coll *mongo.Collection,
	ts bson.Timestamp,
	idxName string,
	expireAfterSeconds int32,
) db.Oplog {
	t.Helper()

	return cmdOp(t, coll, ts, bson.D{
		{"createIndexes", coll.Name()},
		{"v", 2},
		{"key", bson.D{{"createdAt", 1}}},
		{"name", idxName},
		{"expireAfterSeconds", expireAfterSeconds},
	})
}

// collModOp creates "collMod" op which applies mod on the index options.
func collModOp(
	t *testing.T,
	coll *mongo.Collection,
	ts bson.Timestamp,
	idxName string,
	mod bson.D,
) db.Oplog {
	t.Helper()

	return cmdOp(t, coll, ts, bson.D{
		{"collMod", coll.Name()},
		{"index", append(bson.D{{"name", idxName}}, mod...)},
	})
}

// cmdOp creates command ("c") oplog entry for the collection.
// The entry carries the real UUID of the collection, otherwise mongod
// rejects applying it.
func cmdOp(t *testing.T, coll *mongo.Collection, ts bson.Timestamp, cmd bson.D) db.Oplog {
	t.Helper()

	return db.Oplog{
		Timestamp: ts,
		Version:   2,
		Operation: "c",
		Namespace: coll.Database().Name() + ".$cmd",
		UI:        collectionUUID(t, coll),
		Object:    cmd,
	}
}

func collectionUUID(t *testing.T, coll *mongo.Collection) *bson.Binary {
	t.Helper()

	ctx := t.Context()
	cur, err := coll.Database().ListCollections(ctx, bson.D{{"name", coll.Name()}})
	if err != nil {
		t.Fatalf("list collections: %v", err)
	}
	defer cur.Close(ctx)

	if !cur.Next(ctx) {
		t.Fatalf("collection %s not found", coll.Name())
	}

	subtype, data, ok := cur.Current.Lookup("info", "uuid").BinaryOK()
	if !ok {
		t.Fatalf("no uuid for collection %s", coll.Name())
	}

	return &bson.Binary{Subtype: subtype, Data: data}
}

func ts(sec uint32) bson.Timestamp {
	return bson.Timestamp{T: sec, I: 1}
}
