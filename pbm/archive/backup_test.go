package archive

import (
	"context"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var mClient *mongo.Client

func TestMain(m *testing.M) {
	ctx := context.Background()
	mongodbContainer, err := mongodb.Run(ctx, "perconalab/percona-server-mongodb:8.0.4-multi")
	if err != nil {
		log.Fatalf("error while creating mongo test container: %v", err)
	}
	connStr, err := mongodbContainer.ConnectionString(ctx)
	if err != nil {
		log.Fatalf("conn string error: %v", err)
	}
	mClient, err = mongo.Connect(ctx, options.Client().ApplyURI(connStr))
	if err != nil {
		log.Fatalf("mongo client connect error: %v", err)
	}

	code := m.Run()

	err = mClient.Disconnect(ctx)
	if err != nil {
		log.Fatalf("mongo client disconnect error: %v", err)
	}
	if err := testcontainers.TerminateContainer(mongodbContainer); err != nil {
		log.Fatalf("failed to terminate container: %s", err)
	}

	os.Exit(code)
}

func TestListIndexes(t *testing.T) {
	db, coll := "testdb", "testcoll"
	ctx := context.Background()

	_, err := mClient.Database(db).Collection(coll).InsertOne(ctx, bson.M{
		"num":   101,
		"email": "abc@def.com",
	})
	if err != nil {
		t.Fatalf("creating test db: %v", err)
	}
	defer func() { _ = mClient.Database(db).Drop(ctx) }()

	t.Run("single field index", func(t *testing.T) {
		coll := "simple"
		_, err := mClient.Database(db).Collection(coll).InsertOne(ctx, bson.M{
			"num":   101,
			"email": "abc@def.com",
		})
		if err != nil {
			t.Fatalf("creating test collection %s: %v", coll, err)
		}

		expectedKey := bson.D{{"num", int32(1)}}
		indexName, err := mClient.Database(db).Collection(coll).Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: expectedKey,
		})
		if err != nil {
			t.Fatal(err)
		}

		bcp, err := NewBackup(ctx, BackupOptions{
			Client: mClient,
		})
		if err != nil {
			t.Fatal(err)
		}
		idxs, err := bcp.listIndexes(ctx, db, coll)
		if err != nil {
			t.Fatalf("error within listIndexes: %v", err)
		}

		idx, found := findIdx(idxs, indexName)
		if !found {
			t.Fatalf("cannot find index: %s", indexName)
		}
		idxKey, found := findIdxKey(idx)
		if !found {
			t.Fatalf("cannot find index key for index: %s", indexName)
		}
		if !reflect.DeepEqual(expectedKey, idxKey) {
			t.Errorf("index key is wrong: want:%v, got:%v", expectedKey, idxKey)
		}
	})

	t.Run("compound index", func(t *testing.T) {
		coll := "compound"

		_, err := mClient.Database(db).Collection(coll).InsertOne(ctx, bson.M{
			"num":   101,
			"email": "abc@def.com",
			"name":  "test",
		})
		if err != nil {
			t.Fatalf("creating test collection %s: %v", coll, err)
		}

		expectedKey := bson.D{{"num", int32(1)}, {"email", int32(-1)}}
		indexName, err := mClient.Database(db).Collection(coll).Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: expectedKey,
		})
		if err != nil {
			t.Fatal(err)
		}

		bcp, err := NewBackup(ctx, BackupOptions{
			Client: mClient,
		})
		if err != nil {
			t.Fatal(err)
		}
		idxs, err := bcp.listIndexes(ctx, db, coll)
		if err != nil {
			t.Fatalf("error within listIndexes: %v", err)
		}

		idx, found := findIdx(idxs, indexName)
		if !found {
			t.Fatalf("cannot find index: %s", indexName)
		}
		idxKey, found := findIdxKey(idx)
		if !found {
			t.Fatalf("cannot find index key for index: %s", indexName)
		}
		if !reflect.DeepEqual(expectedKey, idxKey) {
			t.Fatalf("index key is wrong: want:%v, got:%v", expectedKey, idxKey)
		}
	})

	t.Run("geospatial index", func(t *testing.T) {
		coll := "geospatial"

		_, err := mClient.Database(db).Collection(coll).InsertOne(ctx, bson.M{
			"num": 101,
			"location": bson.M{
				"type":        "Point",
				"coordinates": []float64{-73.856077, 40.848447},
			},
		})
		if err != nil {
			t.Fatalf("creating test collection %s: %v", coll, err)
		}

		expectedKey := bson.D{{"location", "2dsphere"}}
		indexName, err := mClient.Database(db).Collection(coll).Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: expectedKey,
		})
		if err != nil {
			t.Fatal(err)
		}

		bcp, err := NewBackup(ctx, BackupOptions{
			Client: mClient,
		})
		if err != nil {
			t.Fatal(err)
		}
		idxs, err := bcp.listIndexes(ctx, db, coll)
		if err != nil {
			t.Fatalf("error within listIndexes: %v", err)
		}

		idx, found := findIdx(idxs, indexName)
		if !found {
			t.Fatalf("cannot find index: %s", indexName)
		}
		idxKey, found := findIdxKey(idx)
		if !found {
			t.Fatalf("cannot find index key for index: %s", indexName)
		}
		if !reflect.DeepEqual(expectedKey, idxKey) {
			t.Fatalf("index key is wrong: want:%v, got:%v", expectedKey, idxKey)
		}
		if !findIdxOpt(idx, "2dsphereIndexVersion", int32(3)) {
			t.Fatalf("wrong geospatial option for index: %+v, want option: {Key:2dsphereIndexVersion Value:3}", idx)
		}
	})

	t.Run("text index", func(t *testing.T) {
		coll := "text"

		_, err := mClient.Database(db).Collection(coll).InsertOne(ctx, bson.M{
			"num":  101,
			"desc": "Some test document for text search",
		})
		if err != nil {
			t.Fatalf("creating test collection %s: %v", coll, err)
		}

		expectedKeyRaw := bson.D{{"_fts", "text"}, {"_ftsx", int32(1)}}
		indexName, err := mClient.Database(db).Collection(coll).Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: bson.D{{"desc", "text"}},
		})
		if err != nil {
			t.Fatal(err)
		}

		bcp, err := NewBackup(ctx, BackupOptions{
			Client: mClient,
		})
		if err != nil {
			t.Fatal(err)
		}
		idxs, err := bcp.listIndexes(ctx, db, coll)
		if err != nil {
			t.Fatalf("error within listIndexes: %v", err)
		}

		idx, found := findIdx(idxs, indexName)
		if !found {
			t.Fatalf("cannot find index: %s", indexName)
		}
		idxKey, found := findIdxKey(idx)
		if !found {
			t.Fatalf("cannot find index key for index: %s", indexName)
		}
		if !reflect.DeepEqual(expectedKeyRaw, idxKey) {
			t.Fatalf("index key is wrong: want:%v, got:%v", expectedKeyRaw, idxKey)
		}
		if !findIdxOpt(idx, "weights", bson.D{{"desc", int32(1)}}) {
			t.Fatalf("wrong text option for index: %+v, want option: {Key:weights Value:[{Key:desc Value:1}]}", idx)
		}
		if !findIdxOpt(idx, "default_language", "english") {
			t.Fatalf("wrong text option for index: %+v, want option: {Key:default_language Value:english}", idx)
		}
		if !findIdxOpt(idx, "language_override", "language") {
			t.Fatalf("wrong text option for index: %+v, want option: {Key:language_override Value:language}", idx)
		}
	})

	t.Run("hashed index", func(t *testing.T) {
		coll := "hashed"

		_, err := mClient.Database(db).Collection(coll).InsertOne(ctx, bson.M{
			"num":   101,
			"email": "abc@def.com",
		})
		if err != nil {
			t.Fatalf("creating test collection %s: %v", coll, err)
		}

		expectedKey := bson.D{{"email", "hashed"}}
		indexName, err := mClient.Database(db).Collection(coll).Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys: expectedKey,
		})
		if err != nil {
			t.Fatal(err)
		}

		bcp, err := NewBackup(ctx, BackupOptions{
			Client: mClient,
		})
		if err != nil {
			t.Fatal(err)
		}
		idxs, err := bcp.listIndexes(ctx, db, coll)
		if err != nil {
			t.Fatalf("error within listIndexes: %v", err)
		}

		idx, found := findIdx(idxs, indexName)
		if !found {
			t.Fatalf("cannot find index: %s", indexName)
		}
		idxKey, found := findIdxKey(idx)
		if !found {
			t.Fatalf("cannot find index key for index: %s", indexName)
		}
		if !reflect.DeepEqual(expectedKey, idxKey) {
			t.Fatalf("index key is wrong: want:%v, got:%v", expectedKey, idxKey)
		}
	})

	t.Run("sparse index", func(t *testing.T) {
		coll := "sparse"

		_, err := mClient.Database(db).Collection(coll).InsertOne(ctx, bson.M{
			"num":            101,
			"email":          "abc@def.com",
			"optional_field": "test",
		})
		if err != nil {
			t.Fatalf("creating test collection %s: %v", coll, err)
		}

		expectedKey := bson.D{{"optional_field", int32(1)}}
		indexName, err := mClient.Database(db).Collection(coll).Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    expectedKey,
			Options: options.Index().SetSparse(true),
		})
		if err != nil {
			t.Fatal(err)
		}

		bcp, err := NewBackup(ctx, BackupOptions{
			Client: mClient,
		})
		if err != nil {
			t.Fatal(err)
		}
		idxs, err := bcp.listIndexes(ctx, db, coll)
		if err != nil {
			t.Fatalf("error within listIndexes: %v", err)
		}

		idx, found := findIdx(idxs, indexName)
		if !found {
			t.Fatalf("cannot find index: %s", indexName)
		}
		idxKey, found := findIdxKey(idx)
		if !found {
			t.Fatalf("cannot find index key for index: %s", indexName)
		}
		if !reflect.DeepEqual(expectedKey, idxKey) {
			t.Fatalf("index key is wrong: want:%v, got:%v", expectedKey, idxKey)
		}
		if !findIdxOpt(idx, "sparse", true) {
			t.Fatalf("wrong sparse option for index: %+v, want option: {Key:sparse Value:true}", idx)
		}
	})

	t.Run("partial index", func(t *testing.T) {
		coll := "partial"

		_, err := mClient.Database(db).Collection(coll).InsertOne(ctx, bson.M{
			"num":    101,
			"email":  "abc@def.com",
			"status": "active",
		})
		if err != nil {
			t.Fatalf("creating test collection %s: %v", coll, err)
		}

		expectedKey := bson.D{{"email", int32(1)}}
		partialFilter := bson.D{{"status", "active"}}
		indexName, err := mClient.Database(db).Collection(coll).Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    expectedKey,
			Options: options.Index().SetPartialFilterExpression(partialFilter),
		})
		if err != nil {
			t.Fatal(err)
		}

		bcp, err := NewBackup(ctx, BackupOptions{
			Client: mClient,
		})
		if err != nil {
			t.Fatal(err)
		}
		idxs, err := bcp.listIndexes(ctx, db, coll)
		if err != nil {
			t.Fatalf("error within listIndexes: %v", err)
		}

		idx, found := findIdx(idxs, indexName)
		if !found {
			t.Fatalf("cannot find index: %s", indexName)
		}
		idxKey, found := findIdxKey(idx)
		if !found {
			t.Fatalf("cannot find index key for index: %s", indexName)
		}
		if !reflect.DeepEqual(expectedKey, idxKey) {
			t.Fatalf("index key is wrong: want:%v, got:%v", expectedKey, idxKey)
		}
		if !findIdxOpt(idx, "partialFilterExpression", partialFilter) {
			t.Fatalf("wrong sparse option for index: %+v, "+
				"want option: {Key:partialFilterExpression Value:[{Key:status Value:active}]}", idx)
		}
	})

	t.Run("ttl index", func(t *testing.T) {
		coll := "ttl"

		_, err := mClient.Database(db).Collection(coll).InsertOne(ctx, bson.M{
			"num":       101,
			"email":     "abc@def.com",
			"createdAt": time.Now(),
		})
		if err != nil {
			t.Fatalf("creating test collection %s: %v", coll, err)
		}

		expectedKey := bson.D{{"createdAt", int32(1)}}
		indexName, err := mClient.Database(db).Collection(coll).Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    expectedKey,
			Options: options.Index().SetExpireAfterSeconds(60),
		})
		if err != nil {
			t.Fatal(err)
		}

		bcp, err := NewBackup(ctx, BackupOptions{
			Client: mClient,
		})
		if err != nil {
			t.Fatal(err)
		}
		idxs, err := bcp.listIndexes(ctx, db, coll)
		if err != nil {
			t.Fatalf("error within listIndexes: %v", err)
		}

		idx, found := findIdx(idxs, indexName)
		if !found {
			t.Fatalf("cannot find index: %s", indexName)
		}
		idxKey, found := findIdxKey(idx)
		if !found {
			t.Fatalf("cannot find index key for index: %s", indexName)
		}
		if !reflect.DeepEqual(expectedKey, idxKey) {
			t.Fatalf("index key is wrong: want:%v, got:%v", expectedKey, idxKey)
		}
		if !findIdxOpt(idx, "expireAfterSeconds", int32(60)) {
			t.Fatalf("wrong sparse option for index: %+v, want option: {Key:expireAfterSeconds Value:60}", idx)
		}
	})

	t.Run("unique index", func(t *testing.T) {
		coll := "unique"

		_, err := mClient.Database(db).Collection(coll).InsertOne(ctx, bson.M{
			"num":   101,
			"email": "abc@def.com",
		})
		if err != nil {
			t.Fatalf("creating test collection %s: %v", coll, err)
		}

		expectedKey := bson.D{{"email", int32(1)}}
		indexName, err := mClient.Database(db).Collection(coll).Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    expectedKey,
			Options: options.Index().SetUnique(true),
		})
		if err != nil {
			t.Fatal(err)
		}

		bcp, err := NewBackup(ctx, BackupOptions{
			Client: mClient,
		})
		if err != nil {
			t.Fatal(err)
		}
		idxs, err := bcp.listIndexes(ctx, db, coll)
		if err != nil {
			t.Fatalf("error within listIndexes: %v", err)
		}

		idx, found := findIdx(idxs, indexName)
		if !found {
			t.Fatalf("cannot find index: %s", indexName)
		}
		idxKey, found := findIdxKey(idx)
		if !found {
			t.Fatalf("cannot find index key for index: %s", indexName)
		}
		if !reflect.DeepEqual(expectedKey, idxKey) {
			t.Fatalf("index key is wrong: want:%v, got:%v", expectedKey, idxKey)
		}
		if !findIdxOpt(idx, "unique", true) {
			t.Fatalf("wrong sparse option for index: %+v, want option: {Key:unique Value:true}", idx)
		}
	})
}

func findIdx(idxs []IndexSpec, idxName string) (IndexSpec, bool) {
	var res IndexSpec
	for _, idx := range idxs {
		for _, idxProps := range idx {
			if idxProps.Key == "name" && idxProps.Value.(string) == idxName {
				return idx, true
			}
		}
	}
	return res, false
}

func findIdxKey(idx IndexSpec) (bson.D, bool) {
	for _, idxProps := range idx {
		if idxProps.Key == "key" {
			return idxProps.Value.(bson.D), true
		}
	}
	return bson.D{}, false
}

func findIdxOpt(idx IndexSpec, key string, val any) bool {
	for _, idxProps := range idx {
		if idxProps.Key == key {
			return reflect.DeepEqual(idxProps.Value, val)
		}
	}
	return false
}
