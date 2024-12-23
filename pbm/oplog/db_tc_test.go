package oplog

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var mClient *mongo.Client

func TestMain(m *testing.M) {
	ctx := context.Background()
	mongodbContainer, err := mongodb.Run(ctx, "mongo:6")
	if err != nil {
		fmt.Fprintf(os.Stderr, "error while creating mongo test container: %v", err)
		return
	}
	defer func() {
		if err := testcontainers.TerminateContainer(mongodbContainer); err != nil {
			fmt.Fprintf(os.Stderr, "failed to terminate container: %s", err)
		}
	}()

	connStr, err := mongodbContainer.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "conn string error: %v", err)
		return
	}
	mClient, err = mongo.Connect(ctx, options.Client().ApplyURI(connStr))
	if err != nil {
		fmt.Fprintf(os.Stderr, "mongo client connect error: %v", err)
		return
	}

	m.Run()
}

func TestGetUUIDForNSv2(t *testing.T) {
	t.Run("uuid for existing collection", func(t *testing.T) {
		db := newMDB(mClient)

		tDB, tColl := "my_test_db", "my_test_coll"
		err := mClient.Database(tDB).CreateCollection(context.Background(), tColl)
		if err != nil {
			t.Errorf("create collection err: %v", err)
		}

		uuid, err := db.getUUIDForNS(context.Background(), fmt.Sprintf("%s.%s", tDB, tColl))
		if err != nil {
			t.Errorf("got err=%v", err)
		}
		if uuid.IsZero() {
			t.Error("expected to get uuid for collection")
		}
	})

	t.Run("uuid for not existing collection", func(t *testing.T) {
		db := newMDB(mClient)

		tDB, tColl := "xDB", "yColl"
		uuid, err := db.getUUIDForNS(context.Background(), fmt.Sprintf("%s.%s", tDB, tColl))
		if err != nil {
			t.Errorf("got err=%v", err)
		}
		if !uuid.IsZero() {
			t.Errorf("expected to get zero value for uuid for not existing collection, got=%v", uuid)
		}
	})
}

func TestApplyOps(t *testing.T) {
	db := newMDB(mClient)

	tDB, tColl := "tAODB", "dAOColl"
	if _, err := mClient.Database(tDB).Collection(tColl).InsertOne(context.Background(), bson.D{}); err != nil {
		t.Errorf("insert doc err: %v", err)
	}
	iOps := createInsertSimpleOp(t, fmt.Sprintf("%s.%s", tDB, tColl))

	err := db.applyOps([]any{iOps})
	if err != nil {
		t.Fatalf("error when using applyOps, err=%v", err)
	}
	cnt, err := mClient.Database(tDB).Collection(tColl).CountDocuments(context.Background(), bson.D{})
	if err != nil {
		t.Fatalf("error when counting docs within new collection, err=%v", err)
	}
	if cnt != 2 {
		t.Fatalf("wrong number of docs in new collection, got=%d, want=1", cnt)
	}
}
