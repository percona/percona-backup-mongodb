package storage_test

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/encrypt"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/storage/fs"
)

func TestComputePartSize(t *testing.T) {
	const (
		_  = iota
		KB = 1 << (10 * iota)
		MB
		GB
	)

	const (
		defaultSize = 10 * MB
		minSize     = 5 * MB
		maxParts    = 10000
	)

	tests := []struct {
		name     string
		fileSize int64
		userSize int64
		want     int64
	}{
		{
			name:     "default",
			fileSize: 0,
			userSize: 0,
			want:     defaultSize,
		},
		{
			name:     "user size provided",
			fileSize: 0,
			userSize: 20 * MB,
			want:     20 * MB,
		},
		{
			name:     "user size less than min",
			fileSize: 0,
			userSize: 4 * MB,
			want:     minSize,
		},
		{
			name:     "file size requires larger part size",
			fileSize: 100 * GB,
			userSize: 0,
			want:     100 * GB / maxParts * 15 / 10,
		},
		{
			name:     "file size requires larger part size than user size",
			fileSize: 100 * GB,
			userSize: 10 * MB,
			want:     100 * GB / maxParts * 15 / 10,
		},
		{
			name:     "file size does not require larger part size",
			fileSize: 50 * GB,
			userSize: 0,
			want:     defaultSize,
		},
		{
			name:     "file size with user size",
			fileSize: 50 * GB,
			userSize: 12 * MB,
			want:     12 * MB,
		},
		{
			name:     "zero file size",
			fileSize: 0,
			userSize: 0,
			want:     defaultSize,
		},
		{
			name:     "zero user size",
			fileSize: 100 * GB,
			userSize: 0,
			want:     100 * GB / maxParts * 15 / 10,
		},
		{
			name:     "negative user size",
			fileSize: 0,
			userSize: -1,
			want:     defaultSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := storage.ComputePartSize(tt.fileSize, defaultSize, minSize, maxParts, tt.userSize)
			if got != tt.want {
				t.Errorf("ComputePartSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

var (
	storagePath = flag.String("storage-path", "/mnt/nfs/pbm", "FS/NFS file storage dir path")
	bufSize     = flag.Int("buf-size", 0, "backup buffer size: size of internal write buffer, 0 means no buffer")
	fileSize    = flag.Int64("file-size", 100, "file size in MiB that will be uploaded")
	fName       = flag.String("file-name", "test-file", "upload file name")
	compression = flag.String("compression", string(compress.CompressionTypeNone),
		"compression type: none|gzip|pgzip|snappy|lz4|s2|zstd")
)

// BenchmarkStorageUpload is benchmark for uploading data from the stream.
// Destination for uploade is set by `storage-path`.
// The bench is used to measure upload speed (MiB/sec) for different stream & buffer sizes.
/*
for sz in 10 50 100 500 2000; do
go test -v -run=^$ -bench=BenchmarkStorageUpload ./pbm/storage/ \
	-benchtime=10x \
	-storage-path=/tmp \
	-file-size=$sz \
	-buf-size=$((1*1024*1024))
done
*/
func BenchmarkStorageUpload(b *testing.B) {
	size := *fileSize * storage.MiB // from MiB to B

	fsCfg := &fs.Config{
		Path:           *storagePath,
		BackupBuffSize: *bufSize,
	}
	stg, err := fs.New(fsCfg)
	if err != nil {
		b.Fatalf("create fs storage: %v", err)
	}
	saveBuf := make([]byte, fsCfg.GetBackupBuffSize())
	fsSaveBuf := make([]byte, fsCfg.GetBackupBuffSize())

	cType := compress.CompressionType(*compression)
	src := storage.NewSizedRandomDataSrc(size)

	b.SetBytes(size)

	runDir := time.Now().Format("20060102-150405.000000")
	for b.Loop() {
		fileName := fmt.Sprintf("%s/%s-%d", runDir, *fName, rand.Uint64())

		ts := time.Now()
		sz, err := storage.UploadWithOpts(context.Background(), src, stg, cType, nil,
			encrypt.EncryptionTypeNone, "", fileName, -1, saveBuf, fsSaveBuf)
		if err != nil {
			b.Fatalf("storage upload: %v", err)
		}
		elapsed := time.Since(ts)
		r := storage.Results{
			Size:          sz / storage.MiB,
			BuffSize:      fsCfg.GetBackupBuffSize(),
			Time:          elapsed,
			TransferSpeed: float64(sz/storage.MiB) / elapsed.Seconds(),
		}

		b.Logf("%+v", r)
	}
}
