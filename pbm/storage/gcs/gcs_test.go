package gcs

import (
	gcs "cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
	"io"
	"os"
	"strings"
	"testing"
)

func TestGCS(t *testing.T) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "fsouza/fake-gcs-server",
		ExposedPorts: []string{"4443/tcp"},
		Cmd:          []string{"-backend", "memory", "-scheme", "http", "-port", "4443"},
		WaitingFor:   wait.ForLog("server started at"),
	}

	gcsContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start GCS container: %s", err)
	}
	defer func() {
		if err = testcontainers.TerminateContainer(gcsContainer); err != nil {
			t.Fatalf("failed to terminate container: %s", err)
		}
	}()

	host, err := gcsContainer.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %s", err)
	}
	port, err := gcsContainer.MappedPort(ctx, "4443")
	if err != nil {
		t.Fatalf("failed to get mapped port: %s", err)
	}
	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	_ = os.Setenv("STORAGE_EMULATOR_HOST", endpoint)
	defer func() {
		_ = os.Unsetenv("STORAGE_EMULATOR_HOST")
	}()

	gcsClient, err := gcs.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		t.Fatalf("failed to create GCS client: %s", err)
	}
	defer func() {
		if err = gcsClient.Close(); err != nil {
			t.Fatalf("failed to close client: %s", err)
		}
	}()

	bucketName := "test-bucket"
	bucket := gcsClient.Bucket(bucketName)
	if err = bucket.Create(ctx, "fakeProject", nil); err != nil {
		t.Fatalf("failed to create bucket: %s", err)
	}

	opts := &Config{
		Bucket: bucketName,
		Credentials: Credentials{
			ClientEmail: "email@example.com",
			PrivateKey:  "-----BEGIN PRIVATE KEY-----\nKey\n-----END PRIVATE KEY-----\n",
		},
	}

	stg, err := New(opts, "node", nil)
	if err != nil {
		t.Fatalf("failed to create gcs storage: %s", err)
	}

	t.Run("Type", func(t *testing.T) {
		if stg.Type() != storage.GCS {
			t.Errorf("expected storage type %s, got %s", storage.GCS, stg.Type())
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
			return
			//t.Fatalf("SourceReader failed: %s", err)
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
