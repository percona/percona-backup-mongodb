package status

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	tcetcd "github.com/testcontainers/testcontainers-go/modules/etcd"
)

const etcdImage = "gcr.io/etcd-development/etcd:v3.6.12"

var testEndpoints []string

func TestMain(m *testing.M) {
	ctx := context.Background()

	ctr, err := tcetcd.Run(ctx, etcdImage)
	if err != nil {
		log.Fatalf("start etcd container: %v", err)
	}

	testEndpoints, err = ctr.ClientEndpoints(ctx)
	if err != nil {
		_ = ctr.Terminate(ctx)
		log.Fatalf("get etcd client endpoints: %v", err)
	}

	code := m.Run()

	if err := ctr.Terminate(ctx); err != nil {
		log.Printf("terminate etcd container: %v", err)
	}

	os.Exit(code)
}

func TestPut(t *testing.T) {
	ctx := context.Background()

	t.Run("stores doc at the returned revision", func(t *testing.T) {
		repo := NewRepo(newEtcdClient(t))
		want := testDoc()

		rev, err := repo.Put(ctx, "rs0-0", want)
		if err != nil {
			t.Fatalf("Put: %v", err)
		}

		got, err := repo.Get(ctx, "rs0-0", rev)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got != want {
			t.Errorf("stored doc: got %+v, want %+v", got, want)
		}
	})

	t.Run("each put advances the revision", func(t *testing.T) {
		repo := NewRepo(newEtcdClient(t))

		rev1, err := repo.Put(ctx, "rs0-0", agentDoc{Role: RoleWorker})
		if err != nil {
			t.Fatalf("first Put: %v", err)
		}
		rev2, err := repo.Put(ctx, "rs0-0", agentDoc{Role: RoleWorker, IsLeader: true})
		if err != nil {
			t.Fatalf("second Put: %v", err)
		}
		if rev2 <= rev1 {
			t.Fatalf("revision should advance: first %d, second %d", rev1, rev2)
		}

		got, err := repo.Get(ctx, "rs0-0", rev2)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !got.IsLeader {
			t.Errorf("doc at rev %d should be the second one, got %+v", rev2, got)
		}
	})
}

func TestGet(t *testing.T) {
	ctx := context.Background()

	t.Run("missing returns ErrDocNotFound", func(t *testing.T) {
		cli := newEtcdClient(t)
		repo := NewRepo(cli)
		// any existing revision, the key has never been written
		resp, err := cli.Get(ctx, "any")
		if err != nil {
			t.Fatalf("get current rev: %v", err)
		}

		if _, err := repo.Get(ctx, "ghost", resp.Header.Revision); !errors.Is(err, ErrDocNotFound) {
			t.Fatalf("Get missing: got %v, want ErrDocNotFound", err)
		}
	})

	t.Run("returns doc at the requested revision", func(t *testing.T) {
		repo := NewRepo(newEtcdClient(t))
		want1 := agentDoc{Role: RoleWorker}
		want2 := agentDoc{Role: RoleWorker, IsLeader: true}
		rev1, err := repo.Put(ctx, "rs0-0", want1)
		if err != nil {
			t.Fatalf("first Put: %v", err)
		}
		rev2, err := repo.Put(ctx, "rs0-0", want2)
		if err != nil {
			t.Fatalf("second Put: %v", err)
		}

		for rev, want := range map[int64]agentDoc{rev1: want1, rev2: want2} {
			got, err := repo.Get(ctx, "rs0-0", rev)
			if err != nil {
				t.Fatalf("Get at rev %d: %v", rev, err)
			}
			if got != want {
				t.Errorf("Get at rev %d: got %+v, want %+v", rev, got, want)
			}
		}
	})

	t.Run("deleted doc returns ErrDocNotFound", func(t *testing.T) {
		cli := newEtcdClient(t)
		repo := NewRepo(cli)
		rev, err := repo.Put(ctx, "rs0-0", testDoc())
		if err != nil {
			t.Fatalf("Put: %v", err)
		}
		del, err := cli.Delete(ctx, key("rs0-0"))
		if err != nil {
			t.Fatalf("delete doc: %v", err)
		}

		// before the delete the doc is still readable from etcd history
		if _, err := repo.Get(ctx, "rs0-0", rev); err != nil {
			t.Fatalf("Get before delete: %v", err)
		}
		if _, err := repo.Get(ctx, "rs0-0", del.Header.Revision); !errors.Is(err, ErrDocNotFound) {
			t.Fatalf("Get at delete: got %v, want ErrDocNotFound", err)
		}
	})

	t.Run("compacted revision returns error", func(t *testing.T) {
		cli := newEtcdClient(t)
		repo := NewRepo(cli)
		rev1, err := repo.Put(ctx, "rs0-0", agentDoc{Role: RoleWorker})
		if err != nil {
			t.Fatalf("first Put: %v", err)
		}
		rev2, err := repo.Put(ctx, "rs0-0", agentDoc{Role: RoleWorker, IsLeader: true})
		if err != nil {
			t.Fatalf("second Put: %v", err)
		}
		if _, err := cli.Compact(ctx, rev2); err != nil {
			t.Fatalf("compact: %v", err)
		}

		if _, err := repo.Get(ctx, "rs0-0", rev1); err == nil || errors.Is(err, ErrDocNotFound) {
			t.Fatalf("Get compacted: got %v, want compaction error", err)
		}
	})

	t.Run("malformed doc returns error", func(t *testing.T) {
		cli := newEtcdClient(t)
		repo := NewRepo(cli)
		resp, err := cli.Put(ctx, key("rs0-0"), "{not json")
		if err != nil {
			t.Fatalf("seed malformed doc: %v", err)
		}

		_, err = repo.Get(ctx, "rs0-0", resp.Header.Revision)
		if err == nil || errors.Is(err, ErrDocNotFound) {
			t.Fatalf("Get malformed: got %v, want unmarshal error", err)
		}
	})
}

func testDoc() agentDoc {
	return agentDoc{
		Role:        RoleCtrl,
		IsLeader:    true,
		APIPort:     9000,
		MongoInfo:   MongoInfo{SetName: "rs0", Me: "rs0-0:27017", IsPrimary: true},
		AgentStatus: SubsysStatus{OK: true},
	}
}

// newEtcdClient dials the test etcd and resets the agent keyspace.
func newEtcdClient(t *testing.T) *clientv3.Client {
	t.Helper()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   testEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("dial etcd client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	if _, err := cli.Delete(t.Context(), keyPrefix, clientv3.WithPrefix()); err != nil {
		t.Fatalf("reset agent keys: %v", err)
	}

	return cli
}
