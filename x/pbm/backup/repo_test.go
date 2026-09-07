// PBM 2.x package
package backup

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	tcetcd "github.com/testcontainers/testcontainers-go/modules/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/percona/percona-backup-mongodb/x/pbm/defs"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
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

func TestInsert(t *testing.T) {
	ctx := context.Background()

	t.Run("creates new document", func(t *testing.T) {
		repo := newTestRepo(t)

		if err := repo.Insert(ctx, testMeta("2026-04-14T14:07:00Z")); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		got, err := repo.Get(ctx, "2026-04-14T14:07:00Z")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name != "2026-04-14T14:07:00Z" {
			t.Errorf("Name = %q, want %q", got.Name, "2026-04-14T14:07:00Z")
		}
		if got.Type != defs.LogicalBackup {
			t.Errorf("Type = %q, want %q", got.Type, defs.LogicalBackup)
		}
		if got.Status != defs.StatusDone {
			t.Errorf("Status = %q, want %q", got.Status, defs.StatusDone)
		}
	})

	t.Run("existing name returns ErrAlreadyExists", func(t *testing.T) {
		repo := newTestRepo(t)

		if err := repo.Insert(ctx, testMeta("dup")); err != nil {
			t.Fatalf("first Insert: %v", err)
		}

		err := repo.Insert(ctx, testMeta("dup"))
		if !errors.Is(err, ErrAlreadyExists) {
			t.Fatalf("second Insert: got %v, want ErrAlreadyExists", err)
		}
	})

	t.Run("empty name returns ErrNoName", func(t *testing.T) {
		repo := newTestRepo(t)

		if err := repo.Insert(ctx, testMeta("")); !errors.Is(err, ErrNoName) {
			t.Fatalf("Insert empty name: got %v, want ErrNoName", err)
		}
	})
}

func TestUpdate(t *testing.T) {
	ctx := context.Background()

	t.Run("replaces existing document", func(t *testing.T) {
		repo := newTestRepo(t)

		if err := repo.Insert(ctx, testMeta("bcp")); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		upd := testMeta("bcp")
		upd.Status = defs.StatusError
		upd.Err = "boom"
		if err := repo.Update(ctx, upd); err != nil {
			t.Fatalf("Update: %v", err)
		}

		got, err := repo.Get(ctx, "bcp")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Status != defs.StatusError {
			t.Errorf("Status = %q, want %q", got.Status, defs.StatusError)
		}
		if got.Err != "boom" {
			t.Errorf("Err = %q, want %q", got.Err, "boom")
		}
	})

	t.Run("missing returns ErrNotFound", func(t *testing.T) {
		repo := newTestRepo(t)

		err := repo.Update(ctx, testMeta("ghost"))
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Update missing: got %v, want ErrNotFound", err)
		}
	})

	t.Run("empty name returns ErrNoName", func(t *testing.T) {
		repo := newTestRepo(t)

		if err := repo.Update(ctx, testMeta("")); !errors.Is(err, ErrNoName) {
			t.Fatalf("Update empty name: got %v, want ErrNoName", err)
		}
	})
}

func TestGet(t *testing.T) {
	ctx := context.Background()

	t.Run("missing returns ErrNotFound", func(t *testing.T) {
		repo := newTestRepo(t)

		_, err := repo.Get(ctx, "ghost")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get missing: got %v, want ErrNotFound", err)
		}
	})

	t.Run("empty name returns ErrNoName", func(t *testing.T) {
		repo := newTestRepo(t)

		if _, err := repo.Get(ctx, ""); !errors.Is(err, ErrNoName) {
			t.Fatalf("Get empty name: got %v, want ErrNoName", err)
		}
	})
}

func TestGetAll(t *testing.T) {
	ctx := context.Background()

	t.Run("empty store returns empty slice", func(t *testing.T) {
		repo := newTestRepo(t)

		all, err := repo.GetAll(ctx)
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		if len(all) != 0 {
			t.Fatalf("GetAll: got %d backups, want 0", len(all))
		}
	})

	t.Run("returns all ordered by name", func(t *testing.T) {
		repo := newTestRepo(t)

		// Insert out of order; names are timestamps and GetAll must return
		// them in ascending (chronological) order.
		for _, name := range []string{
			"2026-04-14T14:07:00Z",
			"2026-04-12T09:00:00Z",
			"2026-04-13T22:30:00Z",
		} {
			if err := repo.Insert(ctx, testMeta(name)); err != nil {
				t.Fatalf("Insert %q: %v", name, err)
			}
		}

		all, err := repo.GetAll(ctx)
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}

		got := make([]string, len(all))
		for i, meta := range all {
			got[i] = meta.Name
		}
		want := []string{
			"2026-04-12T09:00:00Z",
			"2026-04-13T22:30:00Z",
			"2026-04-14T14:07:00Z",
		}
		if len(got) != len(want) {
			t.Fatalf("GetAll: got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("GetAll order: got %v, want %v", got, want)
			}
		}
	})
}

func TestDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("removes existing document", func(t *testing.T) {
		repo := newTestRepo(t)

		if err := repo.Insert(ctx, testMeta("tmp")); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		if err := repo.Delete(ctx, "tmp"); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		if _, err := repo.Get(ctx, "tmp"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get after Delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("missing returns ErrNotFound", func(t *testing.T) {
		repo := newTestRepo(t)

		err := repo.Delete(ctx, "ghost")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Delete missing: got %v, want ErrNotFound", err)
		}
	})

	t.Run("empty name returns ErrNoName", func(t *testing.T) {
		repo := newTestRepo(t)

		if err := repo.Delete(ctx, ""); !errors.Is(err, ErrNoName) {
			t.Fatalf("Delete empty name: got %v, want ErrNoName", err)
		}
	})
}

func TestDeleteAll(t *testing.T) {
	ctx := context.Background()

	t.Run("removes every document", func(t *testing.T) {
		repo := newTestRepo(t)

		for _, name := range []string{"a", "b", "c"} {
			if err := repo.Insert(ctx, testMeta(name)); err != nil {
				t.Fatalf("Insert %q: %v", name, err)
			}
		}

		n, err := repo.DeleteAll(ctx)
		if err != nil {
			t.Fatalf("DeleteAll: %v", err)
		}
		if n != 3 {
			t.Errorf("DeleteAll: deleted %d, want 3", n)
		}

		all, err := repo.GetAll(ctx)
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		if len(all) != 0 {
			t.Fatalf("GetAll after DeleteAll: got %d backups, want 0", len(all))
		}
	})

	t.Run("empty store is a no-op", func(t *testing.T) {
		repo := newTestRepo(t)

		n, err := repo.DeleteAll(ctx)
		if err != nil {
			t.Fatalf("DeleteAll on empty store: %v", err)
		}
		if n != 0 {
			t.Errorf("DeleteAll on empty store: deleted %d, want 0", n)
		}
	})
}

// newEtcdClient dials the test etcd and resets the backup keyspace, so each
// test starts from an empty store.
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
		t.Fatalf("reset backup keys: %v", err)
	}

	return cli
}

// newTestRepo builds a repo backed by a fresh etcd keyspace.
func newTestRepo(t *testing.T) *Repo {
	t.Helper()

	return New(newEtcdClient(t))
}

func testMeta(name string) *BackupMeta {
	return &BackupMeta{
		Name:   name,
		Type:   defs.LogicalBackup,
		Status: defs.StatusDone,
	}
}
