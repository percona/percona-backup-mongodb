package fs

import (
	"bytes"
	"io"
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

func TestSave(t *testing.T) {
	t.Run("Save with split-merge middleware", func(t *testing.T) {
		testCases := []struct {
			desc      string
			partSize  int64
			fileSize  int64
			wantParts int
		}{
			{
				desc:      "basic use case for splitting files",
				partSize:  10 * 1024,
				fileSize:  23 * 1024,
				wantParts: 3,
			},
			{
				desc:      "splitting 100s of files",
				partSize:  1 * 1024,
				fileSize:  220*1024 + 555,
				wantParts: 221,
			},
			{
				desc:      "splitting 1000s of files",
				partSize:  1 * 512,
				fileSize:  1100*1024 + 1,
				wantParts: 2201,
			},
			{
				desc:      "file of the same size as part",
				partSize:  5 * 1024 * 1024,
				fileSize:  5 * 1024 * 1024,
				wantParts: 1,
			},
			{
				desc:      "file size is a multiple of part",
				partSize:  12 * 1024 * 1024,
				fileSize:  48 * 1024 * 1024,
				wantParts: 4,
			},
			{
				desc:      "single file little bit bigger than part",
				partSize:  15 * 1024 * 1024,
				fileSize:  15*1024*1024 + 2,
				wantParts: 2,
			},
			{
				desc:      "single file that's a bit smaller than part",
				partSize:  7 * 1024 * 1024,
				fileSize:  7*1024*1024 - 1,
				wantParts: 1,
			},
			{
				desc:      "lots of parts and one smaller part",
				partSize:  7 * 1024,
				fileSize:  490*1024 + 1,
				wantParts: 71,
			},
			{
				desc:      "empty file",
				partSize:  10 * 1024,
				fileSize:  0,
				wantParts: 0,
			},
			{
				desc:      "1 byte file",
				partSize:  14 * 1024,
				fileSize:  1,
				wantParts: 1,
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				tmpDir := setupTestDir(t)
				fs := &FS{root: tmpDir}
				smMW := storage.NewSplitMergeMW(fs, BytesToTB(tC.partSize))

				fName := "test_split"
				fContent := make([]byte, tC.fileSize)
				r := bytes.NewReader(fContent)

				err := smMW.Save(fName, r)
				if err != nil {
					t.Fatalf("error while saving file: %v", err)
				}

				files := getFileWithParts(t, tmpDir, fName)
				if len(files) != tC.wantParts {
					t.Fatalf("wrong number of splitted files: want=%d, got=%d", tC.wantParts, len(files))
				}

				wantSizes := calcPartSizes(tC.partSize, tC.fileSize)
				for i := range len(files) {
					if wantSizes[i] != files[i].Size {
						t.Fatalf("wrong file size for file: %s: want=%d, got=%d", files[i].Name, wantSizes[i], files[i].Size)
					}
				}
			})
		}
	})
}

func TestSourceReader(t *testing.T) {
	t.Run("SourceReader with split-merge middleware", func(t *testing.T) {
		testCases := []struct {
			desc           string
			partSize       int64
			mergedFileSize int64
		}{
			{
				desc:           "basic use case for merging parts",
				partSize:       1024,
				mergedFileSize: 11 * 1024,
			},
			{
				desc:           "merging 100s of parts",
				partSize:       199,
				mergedFileSize: 300*199 + 444,
			},
			{
				desc:           "merging 1000s of parts",
				partSize:       57,
				mergedFileSize: 57*1023 + 15,
			},
			{
				desc:           "single part file smaller than part size",
				partSize:       5 * 1024,
				mergedFileSize: 4*1024 + 123,
			},
			{
				desc:           "single part file, the same size as part size",
				partSize:       2 * 1024 * 1024,
				mergedFileSize: 2 * 1024 * 1024,
			},
			{
				desc:           "merged file size is multiple of part size",
				partSize:       10 * 1024 * 1024,
				mergedFileSize: 5 * 10 * 1024 * 1024,
			},
			{
				desc:           "merged file size is for single byte bigger than part",
				partSize:       50 * 1024,
				mergedFileSize: 50*1024 + 1,
			},
			{
				desc:           "empty file",
				partSize:       20 * 1024,
				mergedFileSize: 0,
			},
			{
				desc:           "1 byte file",
				partSize:       20 * 1024,
				mergedFileSize: 1,
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				tmpDir := setupTestDir(t)
				fs := &FS{root: tmpDir}
				smMW := storage.NewSplitMergeMW(fs, BytesToTB(tC.partSize))

				fName := "test_merge"
				var srcContent []byte
				if tC.mergedFileSize != 0 {
					// create test parts
					srcContent = make([]byte, tC.mergedFileSize)
					r := bytes.NewReader(srcContent)
					err := smMW.Save(fName, r)
					if err != nil {
						t.Fatalf("error while creating test parts: %v", err)
					}
				} else {
					file, err := os.Create(filepath.Join(tmpDir, fName))
					if err != nil {
						t.Fatalf("error creating empty file: %v", err)
					}
					defer file.Close()
				}

				rc, err := smMW.SourceReader(fName)
				if err != nil {
					t.Fatalf("error while invoking SourceReader: %v", err)
				}

				dstContent, err := io.ReadAll(rc)
				if err != nil {
					t.Fatalf("reading merged file: %v", err)
				}

				if tC.mergedFileSize != int64(len(dstContent)) {
					t.Fatalf("wrong file size after merge, want=%d, got=%d", tC.mergedFileSize, len(dstContent))
				}

				if !bytes.Equal(srcContent, dstContent) {
					t.Fatal("merged file content doesn't match")
				}
			})
		}
	})
}

func setupTestDir(t *testing.T) string {
	tmpDir, err := os.MkdirTemp("", "fs-test-*")
	if err != nil {
		t.Fatalf("error while creating setup files: %v", err)
	}

	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	return tmpDir
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
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("error while creating file %s: %v", path, err)
	}
}

func createTestDir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatalf("error while creating dir %s: %v", path, err)
	}
}

func BytesToTB(bytes int64) float64 {
	const TB = 1024 * 1024 * 1024 * 1024
	return float64(bytes) / TB
}

func getFileWithParts(t *testing.T, dir, name string) []storage.FileInfo {
	t.Helper()

	fiParts := []storage.FileInfo{}
	fList, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading dir: %v", err)
	}
	for _, entry := range fList {
		if name == storage.GetBasePart(entry.Name()) {
			info, err := entry.Info()
			if err != nil {
				t.Fatalf("gettting file info: %v", err)
			}
			fiParts = append(fiParts, storage.FileInfo{
				Name: entry.Name(),
				Size: info.Size(),
			})
		}
	}

	// sort by base part first, and then by index
	res := make([]storage.FileInfo, len(fiParts))
	for _, f := range fiParts {
		if f.Name == name {
			res[0] = f
		} else {
			i, err := storage.GetPartIndex(f.Name)
			if err != nil {
				t.Fatalf("getting part index: %v", err)
			}
			res[i] = f
		}
	}

	return res
}

func calcPartSizes(partSize, totalSize int64) []int64 {
	padding := totalSize % partSize
	partsCount := totalSize / partSize

	res := make([]int64, partsCount)
	for i := range partsCount {
		res[i] = partSize
	}
	if padding > 0 {
		res = append(res, padding)
	}

	return res
}
