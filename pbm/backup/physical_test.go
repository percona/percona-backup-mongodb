package backup

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/storage/fs"
)

func TestBackupCursor(t *testing.T) {
	conn := TestEnv.Client.MongoClient()
	ctx := context.Background()
	l := log.DiscardEvent

	t.Run("create cursor", func(t *testing.T) {
		bc := NewBackupCursor(conn, l, bson.D{})
		cur, err := bc.create(ctx, cursorCreateRetries)
		if err != nil {
			t.Fatalf("create should succeed, got error: %v", err)
		}
		if cur == nil {
			t.Fatal("cursor should not be nil")
		}

		defer func() {
			if err := cur.Close(ctx); err != nil {
				t.Errorf("cursor should close without error, got: %v", err)
			}
		}()

		hasNext := cur.TryNext(ctx)
		if !hasNext {
			t.Error("cursor should have at least single document")
		}
	})

	t.Run("create cursor with error", func(t *testing.T) {
		tests := []struct {
			name       string
			errCode    int
			errNum     int
			retryNum   int
			wantErr    error
			wantCurNil bool
		}{
			// Error 50917: oplog rolled over backup cursor
			{
				name:       "50917/pass without error",
				errCode:    oplogRolledOverErrCode,
				errNum:     0,
				retryNum:   3,
				wantErr:    nil,
				wantCurNil: false,
			},
			{
				name:       "50917/pass after single retry",
				errCode:    oplogRolledOverErrCode,
				errNum:     1,
				retryNum:   3,
				wantErr:    nil,
				wantCurNil: false,
			},
			{
				name:       "50917/pass after last retry",
				errCode:    oplogRolledOverErrCode,
				errNum:     2,
				retryNum:   3,
				wantErr:    nil,
				wantCurNil: false,
			},
			{
				name:       "50917/fail for exact number of retries",
				errCode:    oplogRolledOverErrCode,
				errNum:     2,
				retryNum:   2,
				wantErr:    errTriesLimitExceeded,
				wantCurNil: true,
			},
			{
				name:       "50917/fail after all retries",
				errCode:    oplogRolledOverErrCode,
				errNum:     10,
				retryNum:   2,
				wantErr:    errTriesLimitExceeded,
				wantCurNil: true,
			},
			// Error 50915: BackupCursorOpenConflictWithCheckpoint
			{
				name:       "50915/pass without error",
				errCode:    openConflictWithCheckpointErrCode,
				errNum:     0,
				retryNum:   3,
				wantErr:    nil,
				wantCurNil: false,
			},
			{
				name:       "50915/pass after single retry",
				errCode:    openConflictWithCheckpointErrCode,
				errNum:     1,
				retryNum:   100,
				wantErr:    nil,
				wantCurNil: false,
			},
			{
				name:       "50915/pass after last retry",
				errCode:    openConflictWithCheckpointErrCode,
				errNum:     3,
				retryNum:   4,
				wantErr:    nil,
				wantCurNil: false,
			},
			{
				name:       "50915/fail for exact number of retries",
				errCode:    openConflictWithCheckpointErrCode,
				errNum:     2,
				retryNum:   2,
				wantErr:    errTriesLimitExceeded,
				wantCurNil: true,
			},
			{
				name:       "50915/fail after all retries",
				errCode:    openConflictWithCheckpointErrCode,
				errNum:     100,
				retryNum:   2,
				wantErr:    errTriesLimitExceeded,
				wantCurNil: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				setBcpCurErr(t, conn, tt.errCode, tt.errNum)
				defer func() {
					disableBcpCurErr(t, conn)
				}()

				bc := NewBackupCursor(conn, l, bson.D{})
				cur, err := bc.create(ctx, tt.retryNum)

				if err != tt.wantErr {
					t.Fatalf("create error mismatch: want=%v, got=%v", tt.wantErr, err)
				}

				if (cur == nil) != tt.wantCurNil {
					if tt.wantCurNil {
						t.Fatal("cursor should be nil")
					} else {
						t.Fatal("cursor should not be nil")
					}
				}

				if cur != nil {
					defer func() {
						if err := cur.Close(ctx); err != nil {
							t.Errorf("cursor should close without error, got: %v", err)
						}
					}()

					hasNext := cur.TryNext(ctx)
					if !hasNext {
						t.Error("cursor should have at least single document")
					}
				}
			})
		}
	})
}

func setBcpCurErr(t *testing.T, m *mongo.Client, errCode, times int) {
	t.Helper()

	err := m.Database("admin").RunCommand(context.Background(), bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", bson.D{
			{"skip", 0},
			{"times", times},
		}},
		{"data", bson.D{
			{"errorCode", errCode},
			{"failCommands", bson.A{"aggregate"}},
		}},
	}).Err()
	if err != nil {
		t.Fatalf("configureFailPoint set err: %v", err)
	}
}

func disableBcpCurErr(t *testing.T, m *mongo.Client) {
	t.Helper()

	err := m.Database("admin").RunCommand(context.Background(), bson.D{
		{"configureFailPoint", "failCommand"},
		{"mode", "off"},
	}).Err()
	if err != nil {
		t.Fatalf("configureFailPoint disable err: %v", err)
	}
}

var (
	localFile = flag.String("local-file", "", "full path to existing local file")
	localPath = flag.String("local-path", "/tmp", "local path where test file will be generated and uploaded")
	fileSize  = flag.Int64("file-size", 100, "file size in MiB that will be generated and uploaded")
	cleanup   = flag.Bool("cleanup", true, "cleanup generated/uploaded files after the test")

	storagePath = flag.String("storage-path", "/mnt/nfs/pbm", "storage dir path where file will be saved,"+
		" destination name of the file will be unique")
	bufSize = flag.Int("buf-size", 0, "backup buffer size: size of internal write buffer,"+
		" 0 means no PBM level buffer, golang internal will be used")

	compression = flag.String("compression", string(compress.CompressionTypeNone),
		"compression type: none|gzip|pgzip|snappy|lz4|s2|zstd")
	compressionLevel = flag.Int("compression-level", -1, "compression level")
)

// BenchmarkCopyGenFilesToFSStorage generates a local file of size `file-size` in `local-path` dir.
// It's used to measure upload speed (MiB/sec) for different file & buffer sizes.
// Bench starts benchmarking only for the copy operation to FS/NFS storage (`storage-path`).
// For copying, it uses the backup buffer size (`buf-size`). At the end, all generated/copied files are
// removed from the local/backup storage, or optionally left there (`-cleanup=false`)
/*
for sz in 10 50 100 500 2000; do
go test -v -run=^$ -bench=^BenchmarkCopyGenFilesToFSStorage$ ./pbm/backup/ \
	-benchtime=10x \
	-local-path=/tmp \
	-file-size=$sz \
	-storage-path=/mnt/nfs/pbm \
	-buf-size=$((5*1024*1024))
done
*/
func BenchmarkCopyGenFilesToFSStorage(b *testing.B) {
	size := *fileSize * storage.MiB

	fsCfg := &fs.Config{
		Path:           *storagePath,
		BackupBuffSize: *bufSize,
	}
	stg, err := fs.New(fsCfg)
	if err != nil {
		b.Fatalf("create fs storage: %v", err)
	}
	cpBuf := make([]byte, fsCfg.GetBackupBuffSize())
	saveBuf := make([]byte, fsCfg.GetBackupBuffSize())
	fsSaveBuf := make([]byte, fsCfg.GetBackupBuffSize())

	cType := compress.CompressionType(*compression)
	var cLevel *int
	if *compressionLevel != -1 {
		cLevel = compressionLevel
	}

	tmpFile, err := os.CreateTemp(*localPath, "writefile-bench-*")
	if err != nil {
		b.Fatalf("create temp file: %v", err)
	}

	if *cleanup {
		b.Cleanup(func() {
			os.Remove(tmpFile.Name())
		})
	}

	if _, err := io.CopyN(tmpFile, storage.NewSizedRandomDataSrc(size), size); err != nil {
		b.Fatalf("write to temp file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		b.Fatalf("close temp file: %v", err)
	}

	b.SetBytes(size)

	runDir := time.Now().Format("20060102-150405.000000")
	if *cleanup {
		b.Cleanup(func() {
			os.RemoveAll(*storagePath)
		})
	}
	for b.Loop() {
		dst := fmt.Sprintf("%s/%s-%d", runDir, filepath.Base(tmpFile.Name()), rand.Uint64())

		ts := time.Now()
		f, err := writeFile(context.Background(), &File{Name: tmpFile.Name()},
			dst, stg, cType, cLevel, cpBuf, saveBuf, fsSaveBuf)
		if err != nil {
			b.Fatalf("writeFile: %v", err)
		}
		elapsed := time.Since(ts)
		r := storage.Results{
			Size:          f.StgSize / storage.MiB,
			BuffSize:      fsCfg.GetBackupBuffSize(),
			Time:          elapsed,
			TransferSpeed: float64(f.StgSize/storage.MiB) / elapsed.Seconds(),
		}

		b.Logf("%+v", r)
	}
}

// BenchmarkCopyFileToFSStorage copies existing local file (`local-file`) to FS/NFS storage (`storage-path`).
// It's used to bench different backup buffer sizes (`buf-size`) and how does it influence on performance
// and sys calls numbers.
/*
dd if=/dev/urandom of=/tmp/pbm-test-file bs=1M count=1024 status=progress
go test -v -run=^$ -bench=^BenchmarkCopyFileToFSStorage$ ./pbm/backup/ -benchtime=10x \
	-local-file=/tmp/pbm-test-file \
	-storage-path=/mnt/nfs/pbm \
	-buf-size=$((5*1024*1024))
*/
func BenchmarkCopyFileToFSStorage(b *testing.B) {
	if *localFile == "" {
		b.Fatal("-local-file needs to be specified")
	}

	fi, err := os.Stat(*localFile)
	if err != nil {
		b.Fatalf("file stat %s: %v", *localFile, err)
	}
	fSize := fi.Size()
	fName := filepath.Base(*localFile)

	fsCfg := &fs.Config{
		Path:           *storagePath,
		BackupBuffSize: *bufSize,
	}
	stg, err := fs.New(fsCfg)
	if err != nil {
		b.Fatalf("create fs storage: %v", err)
	}
	cpBuf := make([]byte, fsCfg.GetBackupBuffSize())
	saveBuf := make([]byte, fsCfg.GetBackupBuffSize())
	fsSaveBuf := make([]byte, fsCfg.GetBackupBuffSize())

	cType := compress.CompressionType(*compression)
	var cLevel *int
	if *compressionLevel != -1 {
		cLevel = compressionLevel
	}

	b.SetBytes(fSize)

	runDir := time.Now().Format("20060102-150405.000000")
	for b.Loop() {
		dst := fmt.Sprintf("%s/%s-%d", runDir, fName, rand.Uint64())

		ts := time.Now()
		f, err := writeFile(context.Background(), &File{Name: *localFile},
			dst, stg, cType, cLevel, cpBuf, saveBuf, fsSaveBuf)
		if err != nil {
			b.Fatalf("writeFile: %v", err)
		}
		elapsed := time.Since(ts)
		r := storage.Results{
			Size:          f.StgSize / storage.MiB,
			BuffSize:      fsCfg.GetBackupBuffSize(),
			Time:          elapsed,
			TransferSpeed: float64(f.StgSize/storage.MiB) / elapsed.Seconds(),
		}

		b.Logf("%+v", r)
	}
}

func TestTrimFilePrefix(t *testing.T) {
	cases := []struct {
		name       string
		fname      string
		trimPrefix string
		want       string
	}{
		{
			name:       "leading slash left after trim is cleaned",
			fname:      "/data/db/collection.wt",
			trimPrefix: "/data/db",
			want:       "collection.wt",
		},
		{
			name:       "prefix with trailing slash",
			fname:      "/data/db/journal/WiredTigerLog.0001",
			trimPrefix: "/data/db/",
			want:       "journal/WiredTigerLog.0001",
		},
		{
			name:       "no matching prefix keeps relative path",
			fname:      "collection.wt",
			trimPrefix: "/data/db",
			want:       "collection.wt",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := trimFilePrefix(c.fname, c.trimPrefix)
			if got != c.want {
				t.Errorf("trimFilePrefix(%q, %q) = %q, want %q",
					c.fname, c.trimPrefix, got, c.want)
			}
		})
	}
}

func TestPlanUploads(t *testing.T) {
	f := func(name string, off, length, size int64) File {
		return File{Name: name, Off: off, Len: length, Size: size}
	}
	up := func(file File) upItem { return upItem{file: file, upload: true} }
	skip := func(file File) upItem { return upItem{file: file} }

	type planUploadsCase struct {
		name  string
		files []File
		incr  bool
		want  []upItem
	}

	t.Run("physical", func(t *testing.T) {
		cases := []planUploadsCase{
			{
				name:  "nil input",
				files: nil,
				want:  nil,
			},
			{
				name:  "empty slice input",
				files: []File{},
				want:  nil,
			},
			{
				name:  "single file",
				files: []File{f("a", 0, 16, 16)},
				want:  []upItem{up(f("a", 0, 16, 16))},
			},
			{
				name:  "single file",
				files: []File{f("a", 0, 0, 16)},
				want:  []upItem{up(f("a", 0, 0, 16))},
			},
			{
				name: "multiple files",
				files: []File{
					f("a", 0, 0, 16),
					f("b", 0, 0, 32),
					f("c", 0, 0, 64),
				},
				want: []upItem{
					up(f("a", 0, 0, 16)),
					up(f("b", 0, 0, 32)),
					up(f("c", 0, 0, 64)),
				},
			},
			{
				name: "Len==0 file is uploaded, not skipped",
				files: []File{
					f("a", 0, 16, 32),
					f("b", 5, 0, 100),
				},
				want: []upItem{
					up(f("a", 0, 16, 32)),
					up(f("b", 5, 0, 100)),
				},
			},
			{
				name: "Off>=Size file is uploaded, not skipped",
				files: []File{
					f("a", 0, 16, 16),
					f("b", 100, 16, 50),
				},
				want: []upItem{
					up(f("a", 0, 16, 16)),
					up(f("b", 100, 16, 50)),
				},
			},
		}

		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				got := planUploads(tt.files, tt.incr)
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("planUploads want=%+v, got=%+v", tt.want, got)
				}
			})
		}
	})

	t.Run("incremental base", func(t *testing.T) {
		cases := []planUploadsCase{
			{
				name:  "single file, non-empty -> one upload",
				files: []File{f("a", 0, 16, 32)},
				incr:  true,
				want:  []upItem{up(f("a", 0, 16, 32))},
			},
			{
				name: "Off<Size is in-range -> uploaded, not skipped",
				incr: true,
				files: []File{
					f("a", 0, 16, 48),
					f("b", 49, 16, 50),
				},
				want: []upItem{
					up(f("a", 0, 16, 48)),
					up(f("b", 49, 16, 50)),
				},
			},
			{
				name: "coalesce consecutive blocks",
				incr: true,
				files: []File{
					f("a", 0, 16, 48),
					f("a", 16, 16, 48),
					f("a", 32, 16, 48),
				},
				want: []upItem{up(f("a", 0, 48, 48))},
			},
			{
				name: "coalesce consecutive blocks with 2 parts",
				incr: true,
				files: []File{
					f("a", 0, 16, 1024),
					f("a", 16, 16, 1024),
					f("a", 64, 16, 1024),
					f("a", 80, 16, 1024),
				},
				want: []upItem{
					up(f("a", 0, 32, 1024)),
					up(f("a", 64, 32, 1024)),
				},
			},
			{
				name: "same file: two blocks coalesce, gapped third separate",
				incr: true,
				files: []File{
					f("a", 0, 16, 80),
					f("a", 16, 16, 80),
					f("a", 64, 16, 80),
				},
				want: []upItem{
					up(f("a", 0, 32, 80)),
					up(f("a", 64, 16, 80)),
				},
			},
			{
				name: "gap between blocks is not coalesced",
				incr: true,
				files: []File{
					f("a", 0, 16, 80),
					f("a", 48, 16, 80),
				},
				want: []upItem{
					up(f("a", 0, 16, 80)),
					up(f("a", 48, 16, 80)),
				},
			},
			{
				name: "coalesce updates Size to the latest block's Size",
				incr: true,
				files: []File{
					f("a", 0, 16, 100),
					f("a", 16, 16, 200),
				},
				want: []upItem{up(f("a", 0, 32, 200))},
			},
			{
				name: "different files are separate uploads",
				incr: true,
				files: []File{
					f("a", 0, 16, 16),
					f("b", 0, 16, 16),
				},
				want: []upItem{
					up(f("a", 0, 16, 16)),
					up(f("b", 0, 16, 16)),
				},
			},
			{
				name: "coalescing block is broken",
				incr: true,
				files: []File{
					f("a", 0, 16, 64),
					f("a", 16, 16, 64),
					f("b", 0, 16, 16),
					f("a", 32, 16, 64),
					f("a", 48, 16, 64),
				},
				want: []upItem{
					up(f("a", 0, 32, 64)),
					up(f("b", 0, 16, 16)),
					up(f("a", 32, 32, 64)),
				},
			},
		}

		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				got := planUploads(tt.files, tt.incr)
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("planUploads want=%+v, got=%+v", tt.want, got)
				}
			})
		}
	})

	t.Run("incremental", func(t *testing.T) {
		cases := []planUploadsCase{
			{
				name: "unchanged file recorded as skip",
				incr: true,
				files: []File{
					f("a", 0, 16, 48),
					f("b", 0, 0, 100),
					f("c", 0, 16, 16),
				},
				want: []upItem{
					skip(f("b", -1, -1, 100)),
					up(f("a", 0, 16, 48)),
					up(f("c", 0, 16, 16)),
				},
			},
			{
				name: "out-of-range (Off>Size) recorded as skip",
				incr: true,
				files: []File{
					f("a", 0, 16, 48),
					f("b", 100, 16, 50),
				},
				want: []upItem{
					skip(f("b", -1, -1, 50)),
					up(f("a", 0, 16, 48)),
				},
			},
			{
				name: "out-of-range boundary (Off==Size) recorded as skip",
				incr: true,
				files: []File{
					f("a", 0, 16, 48),
					f("b", 50, 16, 50),
				},
				want: []upItem{
					skip(f("b", -1, -1, 50)),
					up(f("a", 0, 16, 48)),
				},
			},
			{
				name:  "unchanged head file is dropped",
				incr:  true,
				files: []File{f("a", 0, 0, 100)},
				want:  []upItem{},
			},
			{
				name: "skip between coalescing blocks preserves coalesce",
				incr: true,
				files: []File{
					f("a", 0, 16, 32),
					f("b", 0, 0, 16),
					f("a", 16, 16, 32),
				},
				want: []upItem{
					skip(f("b", -1, -1, 16)),
					up(f("a", 0, 32, 32)),
				},
			},
			{
				name: "multiple skips and upload",
				incr: true,
				files: []File{
					f("a", 0, 16, 16),
					f("b", 0, 0, 16),
					f("c", 0, 0, 16),
					f("d", 100, 16, 50),
				},
				want: []upItem{
					skip(f("b", -1, -1, 16)),
					skip(f("c", -1, -1, 16)),
					skip(f("d", -1, -1, 50)),
					up(f("a", 0, 16, 16)),
				},
			},
		}

		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				got := planUploads(tt.files, tt.incr)
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("planUploads want=%+v, got=%+v", tt.want, got)
				}
			})
		}
	})
}
