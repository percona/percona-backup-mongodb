package ctrl

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
)

var (
	mClient    *mongo.Client
	connClient connect.Client
)

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

	connClient = connect.UnsafeClient(mClient)

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

func TestListenCmdFiltersDuplicate(t *testing.T) {
	ctx := context.Background()
	coll := connClient.CmdStreamCollection()
	_ = coll.Drop(ctx)

	restoreTS := time.Now().UTC().Unix()
	backupTS := restoreTS + 1

	restoreDoc := bson.D{
		{"_id", primitive.NewObjectID()},
		{"cmd", "restore"},
		{"ts", restoreTS},
	}
	backupDoc := bson.D{
		{"_id", primitive.NewObjectID()},
		{"cmd", "backup"},
		{"ts", backupTS},
	}

	if _, err := coll.InsertOne(ctx, restoreDoc); err != nil {
		t.Fatalf("insert restore: %v", err)
	}
	if _, err := coll.InsertOne(ctx, backupDoc); err != nil {
		t.Fatalf("insert backup: %v", err)
	}

	cmdC, errC := ListenCmd(ctx, connClient, make(chan struct{}))

	var got []Cmd
	for len(got) < 2 { // want only restore + backup
		select {
		case cmd := <-cmdC:
			got = append(got, cmd)
		case err := <-errC:
			t.Fatalf("listener error: %v", err)
		}
	}

	if len(got) != 2 || got[0].Cmd != "restore" || got[1].Cmd != "backup" {
		t.Errorf("duplicate leaked through: %v", got)
	}
}

func TestCheckDuplicateCmd(t *testing.T) {
	c1 := Cmd{Cmd: "backup", TS: 100}
	c2 := Cmd{Cmd: "restore", TS: 100}
	c3 := Cmd{Cmd: "backup", TS: 200}

	buckets := map[int64][]Cmd{
		100: {c1},
	}

	tests := []struct {
		name     string
		cmd      Cmd
		expected bool
	}{
		{"first command, new bucket", c3, false},
		{"different cmd, same ts", c2, false},
		{"exact duplicate", c1, true},
	}

	for _, tt := range tests {
		if got := checkDuplicateCmd(buckets, tt.cmd); got != tt.expected {
			t.Errorf("%s: want %v, got %v", tt.name, tt.expected, got)
		}
	}
}

func TestCleanupOldCmdBuckets(t *testing.T) {
	buckets := map[int64][]Cmd{
		90:  {{Cmd: "backup", TS: 90}},
		100: {{Cmd: "backup", TS: 100}},
		110: {{Cmd: "backup", TS: 110}},
	}

	cleanupOldCmdBuckets(buckets, 100)

	if _, ok := buckets[90]; ok {
		t.Errorf("bucket 90 should have been deleted")
	}
	if _, ok := buckets[100]; !ok {
		t.Errorf("bucket 100 should be kept (ts == lastTS)")
	}
	if _, ok := buckets[110]; !ok {
		t.Errorf("bucket 110 should be kept (ts > lastTS)")
	}
}
