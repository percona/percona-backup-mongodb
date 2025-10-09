package mio

import (
	"context"
	"io"
	"net/url"
	"path"
	"runtime"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func TestMinio(t *testing.T) {
	ctx := context.Background()

	minioContainer, err := tcminio.Run(ctx, "minio/minio:RELEASE.2024-08-17T01-24-54Z")
	defer func() {
		if err := testcontainers.TerminateContainer(minioContainer); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		t.Fatalf("failed to start container: %s", err)
	}

	bucketName := "test-bucket-mio"
	epTC, err := minioContainer.Endpoint(ctx, "http")
	if err != nil {
		t.Fatalf("failed to get endpoint: %s", err)
	}
	u, err := url.Parse(epTC)
	if err != nil {
		t.Fatalf("parsing endpoint: %v", err)
	}
	epMinio := u.Host

	cfg := &Config{
		Endpoint: epMinio,
		Bucket:   bucketName,
		Prefix:   "p1",
		Secure:   false,
		Credentials: Credentials{
			SigVer:          "V4",
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
		},
	}
	minioCl, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.Credentials.AccessKeyID, cfg.Credentials.SecretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		t.Fatalf("minio client creation: %v", err)
	}
	err = minioCl.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{})
	if err != nil {
		t.Errorf("bucket creation: %v", err)
	}

	stg, err := New(cfg, "", nil)
	if err != nil {
		t.Fatalf("storage creation: %v", err)
	}

	storage.RunStorageBaseTests(t, stg, storage.Minio)
	storage.RunStorageAPITests(t, stg)
	storage.RunSplitMergeMWTests(t, stg)

	t.Run("with downloader", func(t *testing.T) {
		stg, err := NewWithDownloader(cfg, "node", nil, 0, 0, 0)
		if err != nil {
			t.Fatalf("failed to create minio storage: %s", err)
		}

		storage.RunStorageBaseTests(t, stg, storage.Minio)
		storage.RunStorageAPITests(t, stg)
		storage.RunSplitMergeMWTests(t, stg)
	})
}

// TestUploadGCS shows how it's possible to upload corrupted file
// without getting any error from the minio library.
//
// To simulate network interruption use:
// tc qdisc add dev eth0 root netem loss 100%
//
// To revert it to normal use:
// tc qdisc del dev eth0 root netem
func TestUploadGCS(t *testing.T) {
	t.Skip("for manual invocation, it will be deleted after GCS HMAC is deprecated")

	ep := "storage.googleapis.com"
	bucket := "gcs-bucket"
	prefix := "test-prefix"
	accessKeyID := "key-id"
	secretAccessKey := "secret-key"

	fname := time.Now().Format("2006-01-02T15:04:05")

	mc, err := minio.New(ep, &minio.Options{
		Creds:  credentials.NewStaticV2(accessKeyID, secretAccessKey, ""),
		Secure: true,
	})
	if err != nil {
		t.Fatalf("minio client creation for GCS: %v", err)
	}
	t.Log("minio client created")

	t.Logf("uploading file: %s", fname)

	infR := NewInfiniteCustomReader()
	r := io.LimitReader(infR, targetSizeBytes)

	putOpts := minio.PutObjectOptions{
		PartSize:   uint64(defaultPartSize),
		NumThreads: uint(max(runtime.NumCPU()/2, 1)),
	}
	info, err := mc.PutObject(
		context.Background(),
		bucket,
		path.Join(prefix, fname),
		r,
		-1,
		putOpts,
	)
	if err != nil {
		t.Fatalf("put object: %v", err)
	}

	t.Logf("upload info: %#v", info)
}

func TestUploadAWSSigV2(t *testing.T) {
	t.Skip("for manual invocation, it will be deleted after GCS HMAC is deprecated")

	ep := "s3.amazonaws.com"
	region := "eu-central-1"
	bucket := "aws-bucket"
	prefix := "test-prefix"
	accessKeyID := "key-id"
	secretAccessKey := "secret-key"

	fname := time.Now().Format("2006-01-02T15:04:05")

	mc, err := minio.New(ep, &minio.Options{
		Region: region,
		Creds:  credentials.NewStaticV2(accessKeyID, secretAccessKey, ""),
		Secure: true,
	})
	if err != nil {
		t.Fatalf("minio client creation for aws: %v", err)
	}
	t.Log("minio client created for aws with sigV2")

	t.Logf("uploading file: %s", fname)

	infR := NewInfiniteCustomReader()
	r := io.LimitReader(infR, targetSizeBytes)

	putOpts := minio.PutObjectOptions{
		PartSize:   uint64(defaultPartSize),
		NumThreads: uint(max(runtime.NumCPU()/2, 1)),
	}
	info, err := mc.PutObject(
		context.Background(),
		bucket,
		path.Join(prefix, fname),
		r,
		-1,
		putOpts,
	)
	if err != nil {
		t.Fatalf("put object: %v", err)
	}

	t.Logf("upload info: %#v", info)
}

func TestUploadAWSSigV4(t *testing.T) {
	t.Skip("for manual invocation, it will be deleted after GCS HMAC is deprecated")

	ep := "s3.amazonaws.com"
	region := "eu-central-1"
	bucket := "aws-bucket"
	prefix := "test-prefix"
	accessKeyID := "key-id"
	secretAccessKey := "secret-key"

	fname := time.Now().Format("2006-01-02T15:04:05")

	mc, err := minio.New(ep, &minio.Options{
		Region: region,
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: true,
	})
	if err != nil {
		t.Fatalf("minio client creation for aws: %v", err)
	}
	t.Log("minio client created for aws with sigV4")

	t.Logf("uploading file: %s ....", fname)

	infR := NewInfiniteCustomReader()
	r := io.LimitReader(infR, targetSizeBytes)

	putOpts := minio.PutObjectOptions{
		PartSize:   uint64(defaultPartSize),
		NumThreads: uint(max(runtime.NumCPU()/2, 1)),
	}
	info, err := mc.PutObject(
		context.Background(),
		bucket,
		path.Join(prefix, fname),
		r,
		-1,
		putOpts,
	)
	if err != nil {
		t.Fatalf("put object: %v", err)
	}

	t.Logf("upload info: %#v", info)
}

const targetSizeBytes = 1000 * 1024 * 1024

type InfiniteCustomReader struct {
	pattern      []byte
	patternIndex int
}

func NewInfiniteCustomReader() *InfiniteCustomReader {
	pattern := []byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22}

	return &InfiniteCustomReader{
		pattern:      pattern,
		patternIndex: 0,
	}
}

func (r *InfiniteCustomReader) Read(p []byte) (int, error) {
	readLen := len(p)

	for i := range readLen {
		p[i] = r.pattern[r.patternIndex]
		r.patternIndex = (r.patternIndex + 1) % len(r.pattern)
	}

	return readLen, nil
}
