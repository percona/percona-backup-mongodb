// PBM 2.x package
package config

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	tcetcd "github.com/testcontainers/testcontainers-go/modules/etcd"

	"github.com/percona/percona-backup-mongodb/x/pbm/config/fs"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage"
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

func newTestSvc(t *testing.T) (*Svc, *resyncMock) {
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
		t.Fatalf("reset config keys: %v", err)
	}

	mock := &resyncMock{}
	return New(cli, mock), mock
}

func testConfig(name, path string) *Config {
	return &Config{
		Name: name,
		Storage: StorageConf{
			Type:       storage.Filesystem,
			Filesystem: &fs.Config{Path: path},
		},
	}
}

func TestUpsert(t *testing.T) {
	ctx := context.Background()

	t.Run("creates new document", func(t *testing.T) {
		svc, _ := newTestSvc(t)

		if err := svc.Upsert(ctx, testConfig("primary", "/backups/primary")); err != nil {
			t.Fatalf("Upsert: %v", err)
		}

		got, err := svc.Get(ctx, "primary")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name != "primary" {
			t.Errorf("Name = %q, want %q", got.Name, "primary")
		}
		if got.Storage.Type != storage.Filesystem {
			t.Errorf("Storage.Type = %q, want %q", got.Storage.Type, storage.Filesystem)
		}
		if got.Storage.Filesystem == nil || got.Storage.Filesystem.Path != "/backups/primary" {
			t.Errorf("Storage.Filesystem = %+v, want Path %q", got.Storage.Filesystem, "/backups/primary")
		}
	})

	t.Run("replaces existing document", func(t *testing.T) {
		svc, _ := newTestSvc(t)

		if err := svc.Upsert(ctx, testConfig("dup", "/a")); err != nil {
			t.Fatalf("first Upsert: %v", err)
		}

		// A second Save replaces the whole document (create-or-replace).
		if err := svc.Upsert(ctx, testConfig("dup", "/b")); err != nil {
			t.Fatalf("second Upsert: %v", err)
		}

		got, err := svc.Get(ctx, "dup")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Storage.Filesystem == nil || got.Storage.Filesystem.Path != "/b" {
			t.Errorf("Storage.Filesystem = %+v, want Path %q", got.Storage.Filesystem, "/b")
		}
	})

	t.Run("defaults empty name to main", func(t *testing.T) {
		svc, _ := newTestSvc(t)

		if err := svc.Upsert(ctx, testConfig("", "/default")); err != nil {
			t.Fatalf("Upsert: %v", err)
		}

		got, err := svc.Get(ctx, "main")
		if err != nil {
			t.Fatalf("Get main: %v", err)
		}
		if got.Name != DefaultConfigName {
			t.Errorf("Name = %q, want %q", got.Name, DefaultConfigName)
		}
	})
}

func TestSave(t *testing.T) {
	ctx := context.Background()

	t.Run("resyncs when no config existed before", func(t *testing.T) {
		svc, mock := newTestSvc(t)

		if err := svc.Save(ctx, testConfig("main", "/a")); err != nil {
			t.Fatalf("Save: %v", err)
		}

		if len(mock.seen) != 1 {
			t.Fatalf("resyncer calls = %d, want 1", len(mock.seen))
		}
		if got := mock.seen[0].Filesystem.Path; got != "/a" {
			t.Errorf("resynced storage path = %q, want %q", got, "/a")
		}
	})

	t.Run("resyncs when storage changes", func(t *testing.T) {
		svc, mock := newTestSvc(t)

		if err := svc.Save(ctx, testConfig("main", "/a")); err != nil {
			t.Fatalf("first Save: %v", err)
		}
		if err := svc.Save(ctx, testConfig("main", "/b")); err != nil {
			t.Fatalf("second Save: %v", err)
		}

		if len(mock.seen) != 2 {
			t.Fatalf("resyncer calls = %d, want 2", len(mock.seen))
		}
		if got := mock.seen[1].Filesystem.Path; got != "/b" {
			t.Errorf("resynced storage path = %q, want %q", got, "/b")
		}
	})

	t.Run("skips resync when only non-storage params change", func(t *testing.T) {
		svc, mock := newTestSvc(t)

		if err := svc.Save(ctx, testConfig("main", "/a")); err != nil {
			t.Fatalf("first Save: %v", err)
		}

		// Same storage path, different (non-storage) parameter.
		cfg := testConfig("main", "/a")
		cfg.PITR = &PITRConf{Enabled: true}
		if err := svc.Save(ctx, cfg); err != nil {
			t.Fatalf("second Save: %v", err)
		}

		if len(mock.seen) != 1 {
			t.Fatalf("resyncer calls = %d, want 1 (storage unchanged)", len(mock.seen))
		}
	})

	t.Run("persists the config", func(t *testing.T) {
		svc, _ := newTestSvc(t)

		if err := svc.Save(ctx, testConfig("main", "/a")); err != nil {
			t.Fatalf("Save: %v", err)
		}

		got, err := svc.Get(ctx, "main")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Storage.Filesystem == nil || got.Storage.Filesystem.Path != "/a" {
			t.Errorf("Storage.Filesystem = %+v, want Path %q", got.Storage.Filesystem, "/a")
		}
	})
}

func TestResync(t *testing.T) {
	ctx := context.Background()

	t.Run("resyncs storage", func(t *testing.T) {
		svc, mock := newTestSvc(t)

		if err := svc.Upsert(ctx, testConfig("main", "/a")); err != nil {
			t.Fatalf("Upsert: %v", err)
		}

		// Two resyncs of the same, unchanged storage must both run.
		if err := svc.Resync(ctx, "main"); err != nil {
			t.Fatalf("first Resync: %v", err)
		}
		if err := svc.Resync(ctx, "main"); err != nil {
			t.Fatalf("second Resync: %v", err)
		}

		if len(mock.seen) != 2 {
			t.Fatalf("resyncer calls = %d, want 2", len(mock.seen))
		}
		if got := mock.seen[1].Filesystem.Path; got != "/a" {
			t.Errorf("resynced storage path = %q, want %q", got, "/a")
		}
	})

	t.Run("missing config returns ErrNotFound and skips resync", func(t *testing.T) {
		svc, mock := newTestSvc(t)

		if err := svc.Resync(ctx, "ghost"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Resync missing: got %v, want ErrNotFound", err)
		}
		if len(mock.seen) != 0 {
			t.Fatalf("resyncer calls = %d, want 0", len(mock.seen))
		}
	})
}

func TestGet(t *testing.T) {
	ctx := context.Background()

	t.Run("missing returns ErrNotFound", func(t *testing.T) {
		svc, _ := newTestSvc(t)

		_, err := svc.Get(ctx, "ghost")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get missing: got %v, want ErrNotFound", err)
		}
	})

	t.Run("empty name resolves to main", func(t *testing.T) {
		svc, _ := newTestSvc(t)

		if err := svc.Upsert(ctx, testConfig("main", "/default")); err != nil {
			t.Fatalf("Upsert: %v", err)
		}

		got, err := svc.Get(ctx, "")
		if err != nil {
			t.Fatalf("Get empty name: %v", err)
		}
		if got.Name != DefaultConfigName {
			t.Errorf("Name = %q, want %q", got.Name, DefaultConfigName)
		}
	})
}

func TestGetAll(t *testing.T) {
	ctx := context.Background()

	t.Run("empty store returns empty slice", func(t *testing.T) {
		svc, _ := newTestSvc(t)

		all, err := svc.GetAll(ctx)
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		if len(all) != 0 {
			t.Fatalf("GetAll: got %d configs, want 0", len(all))
		}
	})

	t.Run("returns all ordered by name", func(t *testing.T) {
		svc, _ := newTestSvc(t)

		for _, name := range []string{"c", "a", "b"} {
			if err := svc.Upsert(ctx, testConfig(name, "/"+name)); err != nil {
				t.Fatalf("Upsert %q: %v", name, err)
			}
		}

		all, err := svc.GetAll(ctx)
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}

		got := make([]string, len(all))
		for i, cfg := range all {
			got[i] = cfg.Name
		}
		want := []string{"a", "b", "c"}
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
		svc, _ := newTestSvc(t)

		if err := svc.Upsert(ctx, testConfig("tmp", "/a")); err != nil {
			t.Fatalf("Upsert: %v", err)
		}

		if err := svc.Delete(ctx, "tmp"); err != nil {
			t.Fatalf("Delete: %v", err)
		}

		if _, err := svc.Get(ctx, "tmp"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get after Delete: got %v, want ErrNotFound", err)
		}
	})

	t.Run("missing returns ErrNotFound", func(t *testing.T) {
		svc, _ := newTestSvc(t)

		err := svc.Delete(ctx, "ghost")
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Delete missing: got %v, want ErrNotFound", err)
		}
	})
}

// resyncMock is a storageResyncer that records the storages it is asked to
// resync.
type resyncMock struct {
	seen []*StorageConf
}

func (s *resyncMock) Resync(_ context.Context, stg *StorageConf) error {
	s.seen = append(s.seen, stg)
	return nil
}
