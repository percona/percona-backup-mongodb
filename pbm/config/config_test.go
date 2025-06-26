package config

import (
	"testing"

	"github.com/percona/percona-backup-mongodb/pbm/storage/azure"
	"github.com/percona/percona-backup-mongodb/pbm/storage/gcs"
	"github.com/percona/percona-backup-mongodb/pbm/storage/s3"
)

func TestIsSameStorage(t *testing.T) {
	t.Run("S3", func(t *testing.T) {
		cfg := &s3.Config{
			Region:         "eu",
			EndpointURL:    "ep.com",
			Bucket:         "b1",
			Prefix:         "p1",
			ForcePathStyle: boolPtr(true),
			Credentials: s3.Credentials{
				AccessKeyID:     "k1",
				SecretAccessKey: "k2",
			},
			UploadPartSize: 1000,
			MaxUploadParts: 10001,
			StorageClass:   "sc",

			InsecureSkipTLSVerify: false,
		}
		eq := &s3.Config{
			Region: "eu",
			Bucket: "b1",
			Prefix: "p1",
		}
		if !cfg.IsSameStorage(eq) {
			t.Errorf("config storage should identify the same instance: cfg=%+v, eq=%+v", cfg, eq)
		}

		neq := cfg.Clone()
		neq.Region = "us"
		if cfg.IsSameStorage(neq) {
			t.Errorf("storage instances has different region: cfg=%+v, eq=%+v", cfg, neq)
		}

		neq = cfg.Clone()
		neq.Bucket = "b2"
		if cfg.IsSameStorage(neq) {
			t.Errorf("storage instances has different bucket: cfg=%+v, eq=%+v", cfg, neq)
		}

		neq = cfg.Clone()
		neq.Prefix = "p2"
		if cfg.IsSameStorage(neq) {
			t.Errorf("storage instances has different prefix: cfg=%+v, eq=%+v", cfg, neq)
		}
	})

	t.Run("Azure", func(t *testing.T) {
		cfg := &azure.Config{
			Account:     "a1",
			Container:   "c1",
			EndpointURL: "az.com",
			Prefix:      "p1",
			Credentials: azure.Credentials{
				Key: "k",
			},
		}

		eq := &azure.Config{
			Account:   "a1",
			Container: "c1",
			Prefix:    "p1",
		}
		if !cfg.IsSameStorage(eq) {
			t.Errorf("config storage should identify the same instance: cfg=%+v, eq=%+v", cfg, eq)
		}

		neq := cfg.Clone()
		neq.Account = "a2"
		if cfg.IsSameStorage(neq) {
			t.Errorf("storage instances has different account: cfg=%+v, eq=%+v", cfg, neq)
		}

		neq = cfg.Clone()
		neq.Container = "c2"
		if cfg.IsSameStorage(neq) {
			t.Errorf("storage instances has different container: cfg=%+v, eq=%+v", cfg, neq)
		}

		neq = cfg.Clone()
		neq.Prefix = "p2"
		if cfg.IsSameStorage(neq) {
			t.Errorf("storage instances has different prefix: cfg=%+v, eq=%+v", cfg, neq)
		}
	})

	t.Run("GCS", func(t *testing.T) {
		cfg := &gcs.Config{
			Bucket: "b1",
			Prefix: "p1",
			Credentials: gcs.Credentials{
				PrivateKey: "abc",
			},
			ChunkSize: 1000,
		}

		eq := &gcs.Config{
			Bucket: "b1",
			Prefix: "p1",
		}
		if !cfg.IsSameStorage(eq) {
			t.Errorf("config storage should identify the same instance: cfg=%+v, eq=%+v", cfg, eq)
		}

		neq := cfg.Clone()
		neq.Bucket = "b2"
		if cfg.IsSameStorage(neq) {
			t.Errorf("storage instances has different bucket: cfg=%+v, eq=%+v", cfg, neq)
		}

		neq = cfg.Clone()
		neq.Prefix = "p2"
		if cfg.IsSameStorage(neq) {
			t.Errorf("storage instances has different prefix: cfg=%+v, eq=%+v", cfg, neq)
		}
	})
}

func boolPtr(b bool) *bool {
	return &b
}
