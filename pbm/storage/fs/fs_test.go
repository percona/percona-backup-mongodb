package fs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func TestList(t *testing.T) {
	t.Run("basic usage", func(t *testing.T) {
		tmpDir := setupTestFiles(t)
		fs := &FS{root: tmpDir}

		testCases := []struct {
			desc      string
			prefix    string
			suffix    string
			wantFiles []storage.FileInfo
		}{
			{
				desc:   "list all non-tmp files",
				prefix: "",
				suffix: "",
				wantFiles: []storage.FileInfo{
					{Name: "file1.txt", Size: 8},
					{Name: "file2.log", Size: 8},
					{Name: "subdir/file4.txt", Size: 8},
					{Name: "subdir/file5.log", Size: 8},
				},
			},
			{
				desc:      "list txt files only",
				prefix:    "",
				suffix:    ".txt",
				wantFiles: []storage.FileInfo{{Name: "file1.txt", Size: 8}, {Name: "subdir/file4.txt", Size: 8}},
			},
			{
				desc:      "list tmp files explicitly",
				prefix:    "",
				suffix:    ".tmp",
				wantFiles: []storage.FileInfo{{Name: "file3.txt.tmp", Size: 8}, {Name: "subdir/file6.txt.tmp", Size: 8}},
			},
			{
				desc:      "List files with prefix only",
				prefix:    "subdir",
				suffix:    "",
				wantFiles: []storage.FileInfo{{Name: "file4.txt", Size: 8}, {Name: "file5.log", Size: 8}},
			},
			{
				desc:      "list files with prefix & suffix",
				prefix:    "subdir",
				suffix:    ".log",
				wantFiles: []storage.FileInfo{{Name: "file5.log", Size: 8}},
			},
			{
				desc:      "non existing prefix",
				prefix:    "nonexistent",
				suffix:    "",
				wantFiles: []storage.FileInfo{},
			},
			{
				desc:      "empty dir",
				prefix:    "empty",
				suffix:    "",
				wantFiles: []storage.FileInfo{},
			},
		}

		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				files, err := fs.List(tC.prefix, tC.suffix)
				if err != nil {
					t.Errorf("got error while executing list: %v", err)

				}

				if len(files) != len(tC.wantFiles) {
					t.Errorf("wrong number of returned files: want:%d, got=%d", len(tC.wantFiles), len(files))
				}

				gotFiles := map[string]int64{}
				for _, f := range files {
					gotFiles[f.Name] = f.Size
				}

				for _, f := range tC.wantFiles {
					size, exists := gotFiles[f.Name]
					if !exists {
						t.Errorf("missing file: %s", f.Name)
					}
					if f.Size != size {
						t.Errorf("wrong file size: want=%d, got=%d", f.Size, size)
					}
				}
			})
		}
	})

	t.Run("list and delete in parallel", func(t *testing.T) {
		tmpDir := setupTestFiles(t)
		fs := &FS{root: tmpDir}

		errCh := make(chan error)
		go func() {
			for range 10 {
				_, err := fs.List("", "")
				if err != nil {
					errCh <- err
					break
				}
			}
		}()

		go func() {
			files, err := fs.List("", "")
			if err != nil {
				t.Errorf("try to fetch deletion list: %v", err)
			}
			for _, f := range files {
				if err := os.Remove(filepath.Join(tmpDir, f.Name)); err != nil {
					t.Errorf("error while deleting: %v", err)
				}
			}
		}()

		wantErr := "getting file info"
		select {
		case err := <-errCh:
			if !strings.Contains(err.Error(), wantErr) {
				t.Fatalf("want err: %s, got=%v", wantErr, err)
			}
		case <-time.After(2 * time.Second):
			t.Log("timed out while waiting for err")
		}
	})
}

func setupTestFiles(t *testing.T) string {
	tmpDir, err := os.MkdirTemp("", "fs-test-*")
	if err != nil {
		t.Fatalf("error while creating setup files: %v", err)
	}

	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	// tmpDir/
	//   - file1.txt
	//   - file2.log
	//   - file3.txt.tmp
	//   - subdir/
	//     - file4.txt
	//     - file5.log
	//     - file6.txt.tmp
	//   - empty/
	createTestFile(t, filepath.Join(tmpDir, "file1.txt"), "content1")
	createTestFile(t, filepath.Join(tmpDir, "file2.log"), "content2")
	createTestFile(t, filepath.Join(tmpDir, "file3.txt.tmp"), "content3")

	createTestDir(t, filepath.Join(tmpDir, "subdir"))
	createTestFile(t, filepath.Join(tmpDir, "subdir", "file4.txt"), "content4")
	createTestFile(t, filepath.Join(tmpDir, "subdir", "file5.log"), "content5")
	createTestFile(t, filepath.Join(tmpDir, "subdir", "file6.txt.tmp"), "content6")

	createTestDir(t, filepath.Join(tmpDir, "empty"))

	return tmpDir
}

func createTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("error while creating file %s: %v", path, err)
	}
}
func createTestDir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0755); err != nil {
		t.Fatalf("error while creating dir %s: %v", path, err)
	}
}
