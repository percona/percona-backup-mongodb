package gcs

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	gcs "cloud.google.com/go/storage"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func TestGCS(t *testing.T) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "fsouza/fake-gcs-server",
		ExposedPorts: []string{"4443/tcp"},
		Cmd:          []string{"-public-host", "localhost:4443", "-scheme", "http", "-port", "4443"},
		WaitingFor:   wait.ForLog("server started at"),
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.PortBindings = nat.PortMap{
				"4443/tcp": {{HostIP: "0.0.0.0", HostPort: "4443"}},
			}
		},
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
	fmt.Println(endpoint)

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

	chunkSize := 1024

	opts := &Config{
		Bucket:    bucketName,
		ChunkSize: chunkSize,
		Credentials: Credentials{
			ClientEmail: "email@example.com",
			PrivateKey:  "-----BEGIN PRIVATE KEY-----\nKey\n-----END PRIVATE KEY-----\n",
		},
		Retryer: &Retryer{
			BackoffInitial:    1,
			BackoffMax:        2,
			BackoffMultiplier: 1,
		},
	}

	stg, err := New(opts, "node", nil)
	if err != nil {
		t.Fatalf("failed to create gcs storage: %s", err)
	}

	storage.RunStorageTests(t, stg, storage.GCS)

	t.Run("Delete fails", func(t *testing.T) {
		name := "not_found.txt"
		err := stg.Delete(name)

		if err == nil {
			t.Errorf("expected error when deleting non-existing file, got nil")
		}
	})
}

func TestConfig(t *testing.T) {
	opts := &Config{
		Bucket: "bucketName",
		Prefix: "prefix",
		Credentials: Credentials{
			ClientEmail: "email@example.com",
			PrivateKey:  "-----BEGIN PRIVATE KEY-----\nKey\n-----END PRIVATE KEY-----\n",
		},
	}

	t.Run("Clone", func(t *testing.T) {
		clone := opts.Clone()
		if clone == opts {
			t.Error("expected clone to be a different pointer")
		}

		if !opts.Equal(clone) {
			t.Error("expected clone to be equal")
		}

		opts.Bucket = "updatedName"
		if opts.Equal(clone) {
			t.Error("expected clone to be unchanged when updating original")
		}
	})

	t.Run("Equal fails", func(t *testing.T) {
		if opts.Equal(nil) {
			t.Error("expected not to be equal other nil")
		}

		clone := opts.Clone()
		clone.Prefix = "updatedPrefix"
		if opts.Equal(clone) {
			t.Error("expected not to be equal when updating prefix")
		}

		clone = opts.Clone()
		clone.Credentials.ClientEmail = "updated@example.com"
		if opts.Equal(clone) {
			t.Error("expected not to be equal when updating credentials")
		}
	})
}

func TestEmptyCredentialsFail(t *testing.T) {
	opts := &Config{
		Bucket: "bucketName",
	}

	_, err := New(opts, "node", nil)

	if err == nil {
		t.Fatalf("expected error when not specifying credentials")
	}

	if !strings.Contains(err.Error(), "required for GCS credentials") {
		t.Errorf("expected required credentials, got %s", err)
	}
}
