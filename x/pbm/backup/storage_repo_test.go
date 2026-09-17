// PBM 2.x package
package backup

import (
	"encoding/json"
	"strings"
	"testing"

	fscfg "github.com/percona/percona-backup-mongodb/x/pbm/config/fs"
	"github.com/percona/percona-backup-mongodb/x/pbm/defs"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage/fs"
)

func TestReadMetadata(t *testing.T) {
	t.Run("reads a stored metadata file", func(t *testing.T) {
		storageRepo, stg := newTestStorageRepo(t)

		want := testMeta("2026-04-14T14:07:00Z")
		want.Status = defs.StatusError
		want.Err = "boom"
		saveMeta(t, stg, want)

		got, err := storageRepo.ReadMetadata(want.Name + defs.MetadataFileSuffix)
		if err != nil {
			t.Fatalf("ReadMetadata: %v", err)
		}
		if got.Name != want.Name {
			t.Errorf("Name = %q, want %q", got.Name, want.Name)
		}
		if got.Type != want.Type {
			t.Errorf("Type = %q, want %q", got.Type, want.Type)
		}
		if got.Status != want.Status {
			t.Errorf("Status = %q, want %q", got.Status, want.Status)
		}
		if got.Err != want.Err {
			t.Errorf("Err = %q, want %q", got.Err, want.Err)
		}
	})

	t.Run("missing file returns error", func(t *testing.T) {
		storageRepo, _ := newTestStorageRepo(t)

		if _, err := storageRepo.ReadMetadata("ghost" + defs.MetadataFileSuffix); err == nil {
			t.Fatal("ReadMetadata of missing file: got nil error, want error")
		}
	})

	t.Run("corrupt file returns error", func(t *testing.T) {
		storageRepo, stg := newTestStorageRepo(t)

		saveFile(t, stg, "corrupt"+defs.MetadataFileSuffix, "{not valid json")

		if _, err := storageRepo.ReadMetadata("corrupt" + defs.MetadataFileSuffix); err == nil {
			t.Fatal("ReadMetadata of corrupt file: got nil error, want error")
		}
	})
}

func TestListBackupMeta(t *testing.T) {
	t.Run("empty storage returns empty slice", func(t *testing.T) {
		storageRepo, _ := newTestStorageRepo(t)

		metas, err := storageRepo.ListBackupMeta()
		if err != nil {
			t.Fatalf("ListBackupMeta: %v", err)
		}
		if len(metas) != 0 {
			t.Fatalf("ListBackupMeta: got %d metas, want 0", len(metas))
		}
	})

	t.Run("returns every metadata file", func(t *testing.T) {
		storageRepo, stg := newTestStorageRepo(t)

		want := []string{
			"2026-04-12T09:00:00Z",
			"2026-04-13T22:30:00Z",
			"2026-04-14T14:07:00Z",
		}
		for _, name := range want {
			saveMeta(t, stg, testMeta(name))
		}

		metas, err := storageRepo.ListBackupMeta()
		if err != nil {
			t.Fatalf("ListBackupMeta: %v", err)
		}

		got := make(map[string]bool, len(metas))
		for _, m := range metas {
			got[m.Name] = true
		}
		if len(got) != len(want) {
			t.Fatalf("ListBackupMeta: got %d distinct metas, want %d", len(got), len(want))
		}
		for _, name := range want {
			if !got[name] {
				t.Errorf("ListBackupMeta: missing %q", name)
			}
		}
	})

	t.Run("skips unreadable metadata files", func(t *testing.T) {
		storageRepo, stg := newTestStorageRepo(t)

		saveMeta(t, stg, testMeta("good"))
		saveFile(t, stg, "bad"+defs.MetadataFileSuffix, "{not valid json")

		metas, err := storageRepo.ListBackupMeta()
		if err != nil {
			t.Fatalf("ListBackupMeta: %v", err)
		}
		if len(metas) != 1 {
			t.Fatalf("ListBackupMeta: got %d metas, want 1", len(metas))
		}
		if metas[0].Name != "good" {
			t.Errorf("ListBackupMeta: got %q, want %q", metas[0].Name, "good")
		}
	})

	t.Run("ignores files without the metadata suffix", func(t *testing.T) {
		storageRepo, stg := newTestStorageRepo(t)

		saveMeta(t, stg, testMeta("meta"))
		saveFile(t, stg, "data.tar", "not a metadata file")

		metas, err := storageRepo.ListBackupMeta()
		if err != nil {
			t.Fatalf("ListBackupMeta: %v", err)
		}
		if len(metas) != 1 {
			t.Fatalf("ListBackupMeta: got %d metas, want 1", len(metas))
		}
		if metas[0].Name != "meta" {
			t.Errorf("ListBackupMeta: got %q, want %q", metas[0].Name, "meta")
		}
	})
}

// newTestStorageRepo builds a storage-backed repo over an fs storage rooted at
// a temp dir, returning the storage so tests can seed it.
func newTestStorageRepo(t *testing.T) (*StorageRepo, storage.Storage) {
	t.Helper()

	stg, err := fs.New(&fscfg.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("create fs storage: %v", err)
	}

	return NewStorageRepo(stg), stg
}

func saveMeta(t *testing.T, stg storage.Storage, meta *BackupMeta) {
	t.Helper()

	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal meta %q: %v", meta.Name, err)
	}
	saveFile(t, stg, meta.Name+defs.MetadataFileSuffix, string(data))
}

func saveFile(t *testing.T, stg storage.Storage, name, content string) {
	t.Helper()

	if err := stg.Save(name, strings.NewReader(content)); err != nil {
		t.Fatalf("save file %q: %v", name, err)
	}
}
