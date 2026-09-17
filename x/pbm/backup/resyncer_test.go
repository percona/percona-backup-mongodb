// PBM 2.x package
package backup

import (
	"context"
	"testing"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
)

func TestSyncBackupList(t *testing.T) {
	ctx := context.Background()

	t.Run("loads backups from storage into etcd", func(t *testing.T) {
		repo := newTestRepo(t)
		storageRepo, stg := newTestStorageRepo(t)

		saveMeta(t, stg, testMeta("2026-04-12T09:00:00Z"))
		saveMeta(t, stg, testMeta("2026-04-14T14:07:00Z"))

		if err := syncBackupList(ctx, repo, storageRepo); err != nil {
			t.Fatalf("syncBackupList: %v", err)
		}

		all, err := repo.GetAll(ctx)
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		got := make([]string, len(all))
		for i, meta := range all {
			got[i] = meta.Name
		}
		want := []string{"2026-04-12T09:00:00Z", "2026-04-14T14:07:00Z"}
		if len(got) != len(want) {
			t.Fatalf("GetAll after sync: got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("GetAll after sync: got %v, want %v", got, want)
			}
		}
	})

	t.Run("replaces stale metadata already in etcd", func(t *testing.T) {
		repo := newTestRepo(t)
		storageRepo, stg := newTestStorageRepo(t)

		// A doc that only exists in etcd must be dropped by the sync.
		if err := repo.Insert(ctx, testMeta("stale")); err != nil {
			t.Fatalf("seed Insert: %v", err)
		}
		saveMeta(t, stg, testMeta("fresh"))

		if err := syncBackupList(ctx, repo, storageRepo); err != nil {
			t.Fatalf("syncBackupList: %v", err)
		}

		if _, err := repo.Get(ctx, "stale"); !errors.Is(err, ErrNotFound) {
			t.Errorf("Get stale after sync: got %v, want ErrNotFound", err)
		}
		if _, err := repo.Get(ctx, "fresh"); err != nil {
			t.Errorf("Get fresh after sync: %v", err)
		}
	})

	t.Run("empty storage clears etcd", func(t *testing.T) {
		repo := newTestRepo(t)
		storageRepo, _ := newTestStorageRepo(t)

		if err := repo.Insert(ctx, testMeta("orphan")); err != nil {
			t.Fatalf("seed Insert: %v", err)
		}

		if err := syncBackupList(ctx, repo, storageRepo); err != nil {
			t.Fatalf("syncBackupList: %v", err)
		}

		all, err := repo.GetAll(ctx)
		if err != nil {
			t.Fatalf("GetAll: %v", err)
		}
		if len(all) != 0 {
			t.Fatalf("GetAll after sync of empty storage: got %d backups, want 0", len(all))
		}
	})
}
