package backup

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
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
