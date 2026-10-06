package restore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/storage/fs"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
)

// End-to-end check of loadNativePITR against a real mongod from $PATH:
//
//	NATIVE_PITR_IT=1 go test -mod=vendor ./pbm/restore -bench=NONE -run NativePITRIntegration -v
//
// snapshot a 1-node replset -> write a PITR window (txn, DDL, big doc) ->
// write it as PITR chunks -> on the snapshot: prepareData's edits +
// loadNativePITR -> native recovery -> dbHash must match the source.
func TestNativePITRIntegration(t *testing.T) {
	if os.Getenv("NATIVE_PITR_IT") == "" {
		t.Skip("set NATIVE_PITR_IT=1 to run against mongod from $PATH")
	}
	ctx := context.Background()
	dir := t.TempDir()
	live, snap := filepath.Join(dir, "live"), filepath.Join(dir, "snap")
	const livePort, snapPort = 28199, 28198

	startMongod(t, livePort, live, "--replSet", "it")
	lc := connectMongod(t, livePort)
	if err := lc.Database("admin").RunCommand(ctx, bson.D{{"replSetInitiate", bson.D{
		{"_id", "it"}, {"members", bson.A{bson.D{{"_id", 0}, {"host", fmt.Sprintf("127.0.0.1:%d", livePort)}}}},
	}}}).Err(); err != nil {
		t.Fatal(err)
	}
	waitPrimary(t, lc)
	tc := lc.Database("t").Collection("c")
	for i := 0; i < 3000; i++ {
		if _, err := tc.InsertOne(ctx, bson.D{{"_id", i}, {"v", 0}}); err != nil {
			t.Fatal(err)
		}
	}
	top := oplogEdge(t, lc, -1)
	stopMongod(t, lc, live)
	if out, err := exec.Command("cp", "-a", live, snap).CombinedOutput(); err != nil {
		t.Fatalf("copy snapshot: %v %s", err, out)
	}
	os.Remove(filepath.Join(snap, "mongod.lock"))

	// the PITR window
	startMongod(t, livePort, live, "--replSet", "it")
	lc = connectMongod(t, livePort)
	waitPrimary(t, lc)
	tc = lc.Database("t").Collection("c")
	mustDo(t, func() error {
		_, err := tc.UpdateMany(ctx, bson.D{{"_id", bson.D{{"$lt", 1000}}}}, bson.D{{"$set", bson.D{{"v", 1}}}})
		return err
	})
	mustDo(t, func() error { _, err := tc.DeleteMany(ctx, bson.D{{"_id", bson.D{{"$gte", 2500}}}}); return err })
	for i := 3000; i < 6000; i++ {
		mustDo(t, func() error { _, err := tc.InsertOne(ctx, bson.D{{"_id", i}, {"v", 2}}); return err })
	}
	mustDo(t, func() error {
		_, err := tc.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{"v", 1}}})
		return err
	})
	mustDo(t, func() error {
		_, err := lc.Database("t").Collection("d").InsertOne(ctx,
			bson.D{{"_id", "big"}, {"pad", strings.Repeat("x", 10<<20)}})
		return err
	})
	mustDo(t, func() error {
		sess, err := lc.StartSession()
		if err != nil {
			return err
		}
		defer sess.EndSession(ctx)
		_, err = sess.WithTransaction(ctx, func(sc context.Context) (any, error) {
			if _, err := tc.UpdateOne(sc, bson.D{{"_id", 1}}, bson.D{{"$set", bson.D{{"v", 99}}}}); err != nil {
				return nil, err
			}
			return lc.Database("t").Collection("e").InsertOne(sc, bson.D{{"_id", 1}})
		})
		return err
	})
	mustDo(t, func() error { return lc.Database("t").Collection("f").Drop(ctx) })
	time.Sleep(11 * time.Second) // let the periodic noop writer add noops to the window
	mustDo(t, func() error { _, err := tc.InsertOne(ctx, bson.D{{"_id", "last"}}); return err })
	target := oplogEdge(t, lc, -1)
	var want bson.M
	if err := lc.Database("t").RunCommand(ctx, bson.D{{"dbHash", 1}}).Decode(&want); err != nil {
		t.Fatal(err)
	}

	// PITR chunks: [bottom, mid] without noops (as the slicer means to),
	// [mid, target] with noops (as the slicer actually writes them)
	stg, err := fs.New(&fs.Config{Path: filepath.Join(dir, "stg")})
	if err != nil {
		t.Fatal(err)
	}
	entries := oplogEntries(t, lc)
	mid := len(entries) - 3000
	writeChunk(t, stg, entries[:mid+1], true)
	writeChunk(t, stg, entries[mid:], false)
	stopMongod(t, lc, live)

	// prepareData on the snapshot
	startMongod(t, snapPort, snap, "--setParameter", "disableLogicalSessionCacheRefresh=true", "--slowms", "100000")
	sc := connectMongod(t, snapPort)
	lcl := sc.Database("local")
	for _, c := range []string{"replset.minvalid", "replset.oplogTruncateAfterPoint", "replset.election", "system.replset"} {
		mustDo(t, func() error { _, err := lcl.Collection(c).DeleteMany(ctx, bson.D{}); return err })
	}
	mustDo(t, func() error {
		_, err := lcl.Collection("replset.minvalid").InsertOne(ctx,
			bson.M{"_id": bson.NewObjectID(), "t": -1, "ts": bson.Timestamp{T: 0, I: 1}})
		return err
	})
	mustDo(t, func() error {
		_, err := lcl.Collection("replset.oplogTruncateAfterPoint").InsertOne(ctx,
			bson.M{"_id": "oplogTruncateAfterPoint", "oplogTruncateAfterPoint": target})
		return err
	})

	r := &PhysRestore{
		stg:       stg,
		restoreTS: target,
		log:       log.DiscardEvent,
		nodeInfo:  &topo.NodeInfo{SetName: "rs01"},
	}
	if err := r.loadNativePITR(ctx, sc); err != nil {
		t.Fatalf("loadNativePITR: %v", err)
	}
	if got := oplogEdge(t, sc, -1); got != target {
		t.Fatalf("oplog top %v, want %v", got, target)
	}
	t.Logf("snapshot top %v, target %v: loaded", top, target)
	stopMongod(t, sc, snap)

	startMongod(t, snapPort, snap,
		"--setParameter", "recoverFromOplogAsStandalone=true",
		"--setParameter", "takeUnstableCheckpointOnShutdown=true",
		"--setParameter", "startupRecoveryForRestore=true",
		"--slowms", "100000")
	sc = connectMongod(t, snapPort)
	var got bson.M
	if err := sc.Database("t").RunCommand(ctx, bson.D{{"dbHash", 1}}).Decode(&got); err != nil {
		t.Fatal(err)
	}
	stopMongod(t, sc, snap)
	if got["md5"] != want["md5"] {
		t.Fatalf("dbHash: restored %v, source %v\nrestored %v\nsource %v", got["md5"], want["md5"], got, want)
	}
	t.Logf("dbHash match: %v", got["md5"])
}

func mustDo(t *testing.T, f func() error) {
	t.Helper()
	if err := f(); err != nil {
		t.Fatal(err)
	}
}

func startMongod(t *testing.T, port int, dbpath string, args ...string) {
	t.Helper()
	if err := os.MkdirAll(dbpath, 0o755); err != nil {
		t.Fatal(err)
	}
	args = append([]string{"--dbpath", dbpath, "--port", fmt.Sprint(port), "--bind_ip", "127.0.0.1",
		"--logpath", dbpath + ".log", "--logappend", "--setParameter", "ttlMonitorEnabled=false"}, args...)
	cmd := exec.Command("mongod", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go cmd.Wait() //nolint:errcheck
	t.Cleanup(func() { _ = cmd.Process.Kill() })
}

func connectMongod(t *testing.T, port int) *mongo.Client {
	t.Helper()
	c, err := mongo.Connect(options.Client().
		ApplyURI(fmt.Sprintf("mongodb://127.0.0.1:%d/?directConnection=true", port)).
		SetServerSelectionTimeout(2 * time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Ping(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	return c
}

func stopMongod(t *testing.T, c *mongo.Client, dbpath string) {
	t.Helper()
	_ = c.Database("admin").RunCommand(context.Background(), bson.D{{"shutdown", 1}}).Err()
	if err := waitMgoShutdown(dbpath); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
}

func waitPrimary(t *testing.T, c *mongo.Client) {
	t.Helper()
	for i := 0; i < 120; i++ {
		var r bson.M
		if err := c.Database("admin").RunCommand(context.Background(), bson.D{{"hello", 1}}).Decode(&r); err == nil &&
			r["isWritablePrimary"] == true {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatal("no primary")
}

func oplogEdge(t *testing.T, c *mongo.Client, dir int) bson.Timestamp {
	t.Helper()
	raw, err := c.Database("local").Collection("oplog.rs").FindOne(context.Background(), bson.D{},
		options.FindOne().SetSort(bson.D{{"$natural", dir}})).Raw()
	if err != nil {
		t.Fatal(err)
	}
	var ts bson.Timestamp
	ts.T, ts.I, _ = raw.Lookup("ts").TimestampOK()
	return ts
}

func oplogEntries(t *testing.T, c *mongo.Client) []bson.Raw {
	t.Helper()
	cur, err := c.Database("local").Collection("oplog.rs").Find(context.Background(), bson.D{},
		options.Find().SetSort(bson.D{{"$natural", 1}}))
	if err != nil {
		t.Fatal(err)
	}
	var out []bson.Raw
	for cur.Next(context.Background()) {
		out = append(out, append(bson.Raw(nil), cur.Current...))
	}
	if err := cur.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func writeChunk(t *testing.T, stg storage.Storage, entries []bson.Raw, dropNoops bool) {
	t.Helper()
	ts := func(d bson.Raw) bson.Timestamp {
		var x bson.Timestamp
		x.T, x.I, _ = d.Lookup("ts").TimestampOK()
		return x
	}
	var buf bytes.Buffer
	w, err := compress.Compress(&buf, compress.CompressionTypeS2, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range entries {
		if dropNoops && d.Lookup("op").StringValue() == string(defs.OperationNoop) {
			continue
		}
		if _, err := w.Write(d); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	name := oplog.FormatChunkFilepath("rs01", ts(entries[0]), ts(entries[len(entries)-1]), compress.CompressionTypeS2)
	if err := stg.Save(name, &buf); err != nil {
		t.Fatal(err)
	}
}
