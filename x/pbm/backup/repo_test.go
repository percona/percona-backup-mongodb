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

func TestModify(t *testing.T) {
	ctx := context.Background()

	t.Run("applies fn and stores the result", func(t *testing.T) {
		repo := newTestRepo(t)

		if err := repo.Insert(ctx, testMeta("bcp")); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		out, err := repo.modify(ctx, "bcp", func(m *BackupMeta) error {
			m.Status = defs.StatusError
			m.Err = "boom"
			return nil
		})
		if err != nil {
			t.Fatalf("modify: %v", err)
		}
		if out.Err != "boom" {
			t.Errorf("returned Err = %q, want %q", out.Err, "boom")
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

	t.Run("retries after a concurrent write", func(t *testing.T) {
		cli := newEtcdClient(t)
		repo := New(cli)

		if err := repo.Insert(ctx, testMeta("bcp")); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		// The first attempt races with a writer that bumps the revision
		// between the read and the write, so Modify must re-read and
		// re-apply instead of clobbering it.
		calls := 0
		out, err := repo.modify(ctx, "bcp", func(m *BackupMeta) error {
			calls++
			if calls == 1 {
				concurrentWrite(t, cli, "bcp", func(m *BackupMeta) {
					m.Size = 42
				})
			}
			m.Err = "boom"
			return nil
		})
		if err != nil {
			t.Fatalf("modify: %v", err)
		}
		if calls != 2 {
			t.Errorf("fn called %d times, want 2", calls)
		}
		if out.Err != "boom" {
			t.Errorf("Err = %q, want %q", out.Err, "boom")
		}
		if out.Size != 42 {
			t.Errorf("Size = %d, want 42: the retry must build on the concurrent write", out.Size)
		}

		got, err := repo.Get(ctx, "bcp")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Size != 42 || got.Err != "boom" {
			t.Errorf("stored meta = (%d, %q), want (42, %q)", got.Size, got.Err, "boom")
		}
	})

	t.Run("gives up after maxModifyAttempts", func(t *testing.T) {
		cli := newEtcdClient(t)
		repo := New(cli)

		if err := repo.Insert(ctx, testMeta("bcp")); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		calls := 0
		_, err := repo.modify(ctx, "bcp", func(m *BackupMeta) error {
			calls++
			concurrentWrite(t, cli, "bcp", func(m *BackupMeta) {
				m.Size++
			})
			m.Err = "boom"
			return nil
		})
		if !errors.Is(err, ErrConflict) {
			t.Fatalf("modify: got %v, want ErrConflict", err)
		}
		if calls != maxModifyAttempts {
			t.Errorf("fn called %d times, want %d", calls, maxModifyAttempts)
		}

		got, err := repo.Get(ctx, "bcp")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Err != "" {
			t.Errorf("Err = %q, want empty: a modify that gave up must not write", got.Err)
		}
	})

	t.Run("fn error aborts and is returned", func(t *testing.T) {
		repo := newTestRepo(t)

		if err := repo.Insert(ctx, testMeta("bcp")); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		errBoom := errors.New("boom")
		_, err := repo.modify(ctx, "bcp", func(m *BackupMeta) error {
			m.Err = "written?"
			return errBoom
		})
		if !errors.Is(err, errBoom) {
			t.Fatalf("modify: got %v, want errBoom", err)
		}

		got, err := repo.Get(ctx, "bcp")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Err != "" {
			t.Errorf("Err = %q, want empty: an aborted modify must not write", got.Err)
		}
	})

	t.Run("document deleted returns ErrNotFound", func(t *testing.T) {
		cli := newEtcdClient(t)
		repo := New(cli)

		if err := repo.Insert(ctx, testMeta("bcp")); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		_, err := repo.modify(ctx, "bcp", func(m *BackupMeta) error {
			if err := repo.Delete(ctx, "bcp"); err != nil && !errors.Is(err, ErrNotFound) {
				t.Fatalf("concurrent Delete: %v", err)
			}
			m.Err = "boom"
			return nil
		})
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("modify: got %v, want ErrNotFound", err)
		}
	})

	t.Run("missing returns ErrNotFound", func(t *testing.T) {
		repo := newTestRepo(t)

		_, err := repo.modify(ctx, "ghost", func(m *BackupMeta) error { return nil })
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("modify missing: got %v, want ErrNotFound", err)
		}
	})

	t.Run("empty name returns ErrNoName", func(t *testing.T) {
		repo := newTestRepo(t)

		_, err := repo.modify(ctx, "", func(m *BackupMeta) error { return nil })
		if !errors.Is(err, ErrNoName) {
			t.Fatalf("modify empty name: got %v, want ErrNoName", err)
		}
	})
}

func TestUpdateRSMeta(t *testing.T) {
	ctx := context.Background()

	t.Run("adds a section that is not there yet", func(t *testing.T) {
		repo := newTestRepo(t)

		if err := repo.Insert(ctx, testMeta("bcp")); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		rs := &BackupReplset{Name: "rs0", Node: "rs0-1", Status: defs.StatusRunning}
		if err := repo.UpdateRSMeta(ctx, "bcp", rs); err != nil {
			t.Fatalf("UpdateRSMeta: %v", err)
		}

		got, err := repo.Get(ctx, "bcp")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(got.Replsets) != 1 {
			t.Fatalf("got %d replsets, want 1", len(got.Replsets))
		}
		if got.Replsets[0].Name != "rs0" {
			t.Errorf("Name = %q, want %q", got.Replsets[0].Name, "rs0")
		}
		if got.Replsets[0].Node != "rs0-1" {
			t.Errorf("Node = %q, want %q", got.Replsets[0].Node, "rs0-1")
		}
	})

	t.Run("replaces the section of the same name", func(t *testing.T) {
		repo := newTestRepo(t)

		if err := repo.Insert(ctx, testMeta("bcp")); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		first := &BackupReplset{Name: "rs0", Status: defs.StatusRunning}
		if err := repo.UpdateRSMeta(ctx, "bcp", first); err != nil {
			t.Fatalf("first UpdateRSMeta: %v", err)
		}

		second := &BackupReplset{Name: "rs0", Status: defs.StatusDone, Size: 42}
		if err := repo.UpdateRSMeta(ctx, "bcp", second); err != nil {
			t.Fatalf("second UpdateRSMeta: %v", err)
		}

		got, err := repo.Get(ctx, "bcp")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(got.Replsets) != 1 {
			t.Fatalf("got %d replsets, want 1: the name must not be duplicated", len(got.Replsets))
		}
		if got.Replsets[0].Status != defs.StatusDone {
			t.Errorf("Status = %q, want %q", got.Replsets[0].Status, defs.StatusDone)
		}
		if got.Replsets[0].Size != 42 {
			t.Errorf("Size = %d, want 42", got.Replsets[0].Size)
		}
	})

	t.Run("leaves other sections and top-level fields alone", func(t *testing.T) {
		repo := newTestRepo(t)

		meta := testMeta("bcp")
		meta.Size = 7
		if err := repo.Insert(ctx, meta); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		// Two agents each write their own section.
		for _, name := range []string{"rs0", "rs1"} {
			rs := &BackupReplset{Name: name, Node: name + "-1", Status: defs.StatusRunning}
			if err := repo.UpdateRSMeta(ctx, "bcp", rs); err != nil {
				t.Fatalf("UpdateRSMeta %s: %v", name, err)
			}
		}

		// rs0 moves on; rs1 must not be touched.
		if err := repo.UpdateRSMeta(ctx, "bcp", &BackupReplset{
			Name:   "rs0",
			Node:   "rs0-1",
			Status: defs.StatusDone,
		}); err != nil {
			t.Fatalf("UpdateRSMeta rs0: %v", err)
		}

		got, err := repo.Get(ctx, "bcp")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(got.Replsets) != 2 {
			t.Fatalf("got %d replsets, want 2", len(got.Replsets))
		}

		byName := map[string]BackupReplset{}
		for _, rs := range got.Replsets {
			byName[rs.Name] = rs
		}
		if byName["rs0"].Status != defs.StatusDone {
			t.Errorf("rs0 Status = %q, want %q", byName["rs0"].Status, defs.StatusDone)
		}
		if byName["rs1"].Status != defs.StatusRunning {
			t.Errorf("rs1 Status = %q, want %q", byName["rs1"].Status, defs.StatusRunning)
		}
		if byName["rs1"].Node != "rs1-1" {
			t.Errorf("rs1 Node = %q, want %q", byName["rs1"].Node, "rs1-1")
		}
		if got.Size != 7 {
			t.Errorf("top-level Size = %d, want 7", got.Size)
		}
		if got.Status != defs.StatusDone {
			t.Errorf("top-level Status = %q, want %q", got.Status, defs.StatusDone)
		}
	})

	t.Run("survives a concurrent write to another section", func(t *testing.T) {
		cli := newEtcdClient(t)
		repo := New(cli)

		if err := repo.Insert(ctx, testMeta("bcp")); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		// rs1 lands between rs0's read and write, so rs0 retries and both
		// sections end up stored.
		concurrent := false
		_, err := repo.modify(ctx, "bcp", func(m *BackupMeta) error {
			if !concurrent {
				concurrent = true
				concurrentWrite(t, cli, "bcp", func(*BackupMeta) {})
				if err := repo.UpdateRSMeta(ctx, "bcp", &BackupReplset{Name: "rs1"}); err != nil {
					t.Fatalf("concurrent UpdateRSMeta: %v", err)
				}
			}
			m.Replsets = append(m.Replsets, BackupReplset{Name: "rs0"})
			return nil
		})
		if err != nil {
			t.Fatalf("modify: %v", err)
		}

		got, err := repo.Get(ctx, "bcp")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if len(got.Replsets) != 2 {
			t.Fatalf("got %d replsets, want 2: a concurrent section was lost", len(got.Replsets))
		}
	})

	t.Run("missing backup returns ErrNotFound", func(t *testing.T) {
		repo := newTestRepo(t)

		err := repo.UpdateRSMeta(ctx, "ghost", &BackupReplset{Name: "rs0"})
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("UpdateRSMeta missing: got %v, want ErrNotFound", err)
		}
	})

	t.Run("empty backup name returns ErrNoName", func(t *testing.T) {
		repo := newTestRepo(t)

		err := repo.UpdateRSMeta(ctx, "", &BackupReplset{Name: "rs0"})
		if !errors.Is(err, ErrNoName) {
			t.Fatalf("UpdateRSMeta empty backup name: got %v, want ErrNoName", err)
		}
	})

	t.Run("nil replset returns ErrNoRSName", func(t *testing.T) {
		repo := newTestRepo(t)

		if err := repo.Insert(ctx, testMeta("bcp")); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		err := repo.UpdateRSMeta(ctx, "bcp", nil)
		if !errors.Is(err, ErrNoRSName) {
			t.Fatalf("UpdateRSMeta nil replset: got %v, want ErrNoRSName", err)
		}
	})

	t.Run("empty replset name returns ErrNoRSName", func(t *testing.T) {
		repo := newTestRepo(t)

		if err := repo.Insert(ctx, testMeta("bcp")); err != nil {
			t.Fatalf("Insert: %v", err)
		}

		err := repo.UpdateRSMeta(ctx, "bcp", &BackupReplset{})
		if !errors.Is(err, ErrNoRSName) {
			t.Fatalf("UpdateRSMeta empty replset name: got %v, want ErrNoRSName", err)
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

// concurrentWrite rewrites the stored backup metadata through a second repo
func concurrentWrite(t *testing.T, cli *clientv3.Client, name string, fn func(*BackupMeta)) {
	t.Helper()

	other := New(cli)

	_, err := other.modify(t.Context(), name, func(m *BackupMeta) error {
		fn(m)
		return nil
	})
	if err != nil {
		t.Fatalf("concurrent modify: %v", err)
	}
}
