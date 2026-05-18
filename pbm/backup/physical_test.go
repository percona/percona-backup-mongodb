package backup

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
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
	fsPath         = flag.String("fs-path", "/mnt/nfs/pbm", "storage path where file will be saved")
	backupBuffSize = flag.Int("buff-size", 0, "backup buffer size: size of internal write buffer, 0 means no buffer")
	fSize          = flag.Int64("file-size", 100, "file size in MiB that will be uploaded")
	fName          = flag.String("file-name", "test-file", "upload file name")
	compression    = flag.String("compression", string(compress.CompressionTypeNone),
		"compression type: none|gzip|pgzip|snappy|lz4|s2|zstd")
	compressionLevel = flag.Int("compression-level", -1, "compression level")
)

/*
for sz in 10 50 100 500 2000; do
go test -v -run=^$ -bench=BenchmarkWriteFile -benchtime=10x ./pbm/backup/ -file-size=$sz -buff-size=$((1*1024*1024))
done
*/
func BenchmarkWriteFile(b *testing.B) {
	MiB := int64(1024 * 1024)
	size := *fSize * MiB

	fsCfg := &fs.Config{
		Path:           *fsPath,
		BackupBuffSize: *backupBuffSize,
	}
	stg, err := fs.New(fsCfg)
	if err != nil {
		b.Fatalf("create fs storage: %v", err)
	}

	cType := compress.CompressionType(*compression)
	var cLevel *int
	if *compressionLevel != -1 {
		cLevel = compressionLevel
	}

	tmpFile, err := os.CreateTemp("", "writefile-bench-*")
	if err != nil {
		b.Fatalf("create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := io.CopyN(tmpFile, storage.NewSizedRandomDataSrc(size), size); err != nil {
		b.Fatalf("write to temp file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		b.Fatalf("close temp file: %v", err)
	}

	b.SetBytes(size)

	runDir := time.Now().Format("20060102-150405.000000")
	for b.Loop() {
		dst := fmt.Sprintf("%s/%s-%d", runDir, *fName, rand.Uint64())

		ts := time.Now()
		f, err := writeFile(context.Background(), File{Name: tmpFile.Name()}, dst, stg, cType, cLevel)
		if err != nil {
			b.Fatalf("writeFile: %v", err)
		}
		elapsed := time.Since(ts)
		r := storage.Results{
			Size:        f.StgSize / MiB,
			BuffSize:    fsCfg.GetBackupBuffSize(),
			Time:        elapsed,
			UploadSpeed: float64(f.StgSize/MiB) / elapsed.Seconds(),
		}

		b.Logf("%+v", r)
	}
}
