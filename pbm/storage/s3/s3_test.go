package s3

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func TestS3(t *testing.T) {
	ctx := context.Background()

	minioContainer, err := minio.Run(ctx, "minio/minio:RELEASE.2024-08-17T01-24-54Z")
	defer func() {
		if err := testcontainers.TerminateContainer(minioContainer); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	if err != nil {
		t.Fatalf("failed to start container: %s", err)
	}

	endpoint, err := minioContainer.Endpoint(ctx, "http")
	if err != nil {
		t.Fatalf("failed to get endpoint: %s", err)
	}

	defaultConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithBaseEndpoint(endpoint),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""),
		),
	)

	if err != nil {
		t.Fatalf("failed to load config: %s", err)
	}

	s3Client := s3.NewFromConfig(defaultConfig, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	bucketName := "test-bucket"
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucketName)})
	if err != nil {
		t.Errorf("failed to create bucket: %s", err)
	}

	opts := &Config{
		EndpointURL: endpoint,
		Bucket:      bucketName,
		Credentials: Credentials{
			AccessKeyID:     "minioadmin",
			SecretAccessKey: "minioadmin",
		},
	}

	stg, err := New(opts, "node", nil)
	if err != nil {
		t.Fatalf("failed to create s3 storage: %s", err)
	}

	t.Run("Type", func(t *testing.T) {
		if stg.Type() != storage.S3 {
			t.Errorf("expected storage type %s, got %s", storage.S3, stg.Type())
		}
	})

	t.Run("Save and FileStat", func(t *testing.T) {
		name := "test.txt"
		content := "content"

		err := stg.Save(name, strings.NewReader(content), int64(len(content)))
		if err != nil {
			t.Fatalf("Save failed: %s", err)
		}

		f, err := stg.FileStat(name)
		if err != nil {
			t.Errorf("FileStat failed: %s", err)
		}

		if f.Size != int64(len(content)) {
			t.Errorf("expected size %d, got %d", len(content), f.Size)
		}
	})

	t.Run("List", func(t *testing.T) {
		filesToSave := []struct {
			name    string
			content string
		}{
			{"file1.txt", "content1"},
			{"file2.log", "content1"},
			{"dir/file3.txt", "content3"},
		}

		for _, f := range filesToSave {
			if err := stg.Save(f.name, strings.NewReader(f.content), int64(len(f.content))); err != nil {
				t.Fatalf("Save failed: %s", err)
			}
		}

		files, err := stg.List("", ".txt")
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		// expect "file1.txt", "dir/file3.txt" and "test.txt" from previous test to be returned
		if len(files) != 3 {
			t.Errorf("expected 3 .txt files, got %d", len(files))
		}

		files, err = stg.List("dir", ".txt")
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		// expect "dir/file3.txt" to be returned
		if len(files) != 1 {
			t.Errorf("expected 1 .txt files, got %d", len(files))
		}
	})

	t.Run("Delete", func(t *testing.T) {
		name := "delete.txt"
		content := "content"

		if err := stg.Save(name, strings.NewReader(content), int64(len(content))); err != nil {
			t.Fatalf("Save failed: %s", err)
		}

		err := stg.Delete(name)
		if err != nil {
			t.Fatalf("Delete failed: %s", err)
		}

		f, err := stg.FileStat(name)
		if err == nil {
			t.Errorf("expected error for deleted file, got nil")
		}

		if f != (storage.FileInfo{}) {
			t.Errorf("expected %v, got %v", storage.FileInfo{}, f)
		}
	})

	t.Run("Copy", func(t *testing.T) {
		src := "copysrc.txt"
		dst := "copydst.txt"
		content := "copy content"

		if err := stg.Save(src, strings.NewReader(content), int64(len(content))); err != nil {
			t.Fatalf("Save failed: %s", err)
		}

		if err := stg.Copy(src, dst); err != nil {
			t.Fatalf("Copy failed: %s", err)
		}

		f, err := stg.FileStat(dst)
		if err != nil {
			t.Errorf("FileStat failed: %s", err)
		}

		if f.Size != int64(len(content)) {
			t.Errorf("expected size %d, got %d", len(content), f.Size)
		}
	})

	t.Run("SourceReader", func(t *testing.T) {
		name := "reader.txt"
		content := "source reader content"

		if err := stg.Save(name, strings.NewReader(content), int64(len(content))); err != nil {
			t.Fatalf("Save failed: %s", err)
		}

		reader, err := stg.SourceReader(name)
		if err != nil {
			t.Fatalf("SourceReader failed: %s", err)
		}

		defer func() {
			err := reader.Close()
			if err != nil {
				t.Fatalf("close failed: %s", err)
			}
		}()

		data, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("ReadAll failed: %s", err)
		}

		if string(data) != content {
			t.Errorf("expected content %s, got %s", content, string(data))
		}
	})
}
