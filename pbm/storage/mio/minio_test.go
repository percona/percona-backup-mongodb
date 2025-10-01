package mio

import (
	"context"
	"net/url"
	"testing"

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
		EndpointURL: epMinio,
		Bucket:      bucketName,
		Prefix:      "p1",
		Secure:      false,
		Credentials: Credentials{
			SigVer:          "V4",
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
		},
	}
	minioCl, err := minio.New(cfg.EndpointURL, &minio.Options{
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
			t.Fatalf("failed to create s3 storage: %s", err)
		}

		storage.RunStorageBaseTests(t, stg, storage.Minio)
		storage.RunStorageAPITests(t, stg)
		storage.RunSplitMergeMWTests(t, stg)
	})
}
