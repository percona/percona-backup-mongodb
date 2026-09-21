package oplog

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
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
	mClient, err = mongo.Connect(options.Client().ApplyURI(connStr))
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

func TestApplyOpsBypassesDocumentValidation(t *testing.T) {
	ctx := t.Context()
	database := mClient.Database("apply_ops_validation")
	t.Cleanup(func() {
		require.NoError(t, database.Drop(context.Background()))
	})

	// Start with a valid document and a rule rejecting negative fieldX values.
	err := database.CreateCollection(ctx, "c1", options.CreateCollection().
		SetValidator(bson.D{{"fieldX", bson.D{{"$gte", 0}}}}).
		SetValidationLevel("strict"))
	require.NoError(t, err)
	coll := database.Collection("c1")
	_, err = coll.InsertOne(ctx, bson.D{{"_id", 1}, {"fieldX", 1}})
	require.NoError(t, err)

	// Replay 1 -> -1 -> 2 through PBM's wrapper. The intermediate value is invalid,
	// but must actually be applied rather than skipped before reaching the target.
	db := newMDB(mClient)
	filter := bson.D{{"_id", 1}}
	for _, value := range []int32{-1, 2} {
		err = db.applyOps([]any{bson.D{
			{"op", "u"},
			{"ns", database.Name() + ".c1"},
			{"o2", filter},
			{"o", bson.D{
				{"$v", 2},
				{"diff", bson.D{{"u", bson.D{{"fieldX", value}}}}},
			}},
		}})
		require.NoError(t, err)

		var restored bson.M
		require.NoError(t, coll.FindOne(ctx, filter).Decode(&restored))
		require.Equal(t, value, restored["fieldX"])
	}
}
