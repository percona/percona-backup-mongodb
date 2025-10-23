package config

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/storage/azure"
	"github.com/percona/percona-backup-mongodb/pbm/storage/fs"
	"github.com/percona/percona-backup-mongodb/pbm/storage/gcs"
	"github.com/percona/percona-backup-mongodb/pbm/storage/mio"
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

	t.Run("FS", func(t *testing.T) {
		maxObjSizeGB := 5.5
		cfg := &fs.Config{
			Path:         "a/b/c",
			MaxObjSizeGB: &maxObjSizeGB,
		}

		eq := &fs.Config{
			Path: "a/b/c",
		}
		if !cfg.IsSameStorage(eq) {
			t.Errorf("config storage should identify the same instance: cfg=%+v, eq=%+v", cfg, eq)
		}

		maxObjSizeGB = 2.2
		eq.MaxObjSizeGB = &maxObjSizeGB
		if !cfg.IsSameStorage(eq) {
			t.Errorf("config storage should identify the same instance: cfg=%+v, eq=%+v", cfg, eq)
		}

		neq := cfg.Clone()
		neq.Path = "z/y/x"
		if cfg.IsSameStorage(neq) {
			t.Errorf("storage instances has different bucket: cfg=%+v, eq=%+v", cfg, neq)
		}
	})

	t.Run("minio", func(t *testing.T) {
		cfg := &mio.Config{
			Region:   "eu",
			Endpoint: "ep.com",
			Bucket:   "b1",
			Prefix:   "p1",
			Credentials: mio.Credentials{
				AccessKeyID:     "k1",
				SecretAccessKey: "k2",
				SessionToken:    "sess",
			},
			Secure:   true,
			PartSize: 6 << 20,
			Retryer:  &mio.Retryer{},
		}
		eq := &mio.Config{
			Region:   "eu",
			Endpoint: "ep.com",
			Bucket:   "b1",
			Prefix:   "p1",
		}
		if !cfg.IsSameStorage(eq) {
			t.Errorf("config storage should identify the same instance: cfg=%+v, eq=%+v, diff=%s",
				cfg, eq, cmp.Diff(*cfg, *eq))
		}

		neq := cfg.Clone()
		neq.Region = "us"
		if cfg.IsSameStorage(neq) {
			t.Errorf("storage instances has different region: cfg=%+v, eq=%+v", cfg, neq)
		}

		neq = cfg.Clone()
		neq.Endpoint = "ep2.com"
		if cfg.IsSameStorage(neq) {
			t.Errorf("storage instances has different EndpointURL: cfg=%+v, eq=%+v", cfg, neq)
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
}

func TestCastError(t *testing.T) {
	t.Run("S3", func(t *testing.T) {
		cfg := StorageConf{Type: storage.S3}

		err := cfg.Cast()
		if err == nil {
			t.Errorf("Cast did not raise an error")
		}

	})
}

var connClient connect.Client

func TestMain(m *testing.M) {
	ctx := context.Background()
	mongodbContainer, err := mongodb.Run(ctx, "perconalab/percona-server-mongodb:8.0.4-multi",
		mongodb.WithReplicaSet("rs1"))
	if err != nil {
		log.Fatalf("error while creating mongo test container: %v", err)
	}
	connStr, err := mongodbContainer.ConnectionString(ctx)
	if err != nil {
		log.Fatalf("conn string error: %v", err)
	}
	connStr += "&directConnection=true"
	mClient, err := mongo.Connect(ctx, options.Client().ApplyURI(connStr))
	if err != nil {
		log.Fatalf("mongo client connect error: %v", err)
	}
	err = mClient.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatalf("conn string: %s, ping: %v", connStr, err)
	}

	connClient = connect.UnsafeClient(mClient)

	code := m.Run()

	err = mClient.Disconnect(ctx)
	if err != nil {
		log.Fatalf("mongo client disconnect error: %v", err)
	}
	if err := testcontainers.TerminateContainer(mongodbContainer); err != nil {
		log.Fatalf("failed to terminate container: %s", err)
	}

	os.Exit(code)
}

func TestConfig(t *testing.T) {
	ctx := context.Background()

	t.Run("gcs config", func(t *testing.T) {
		wantCfg := &Config{
			Storage: StorageConf{
				Type: storage.GCS,
				GCS: &gcs.Config{
					Bucket: "b1",
					Prefix: "p1",
					Credentials: gcs.Credentials{
						ClientEmail: "ce1",
						PrivateKey:  "pk1",
					},
					ChunkSize:    100,
					MaxObjSizeGB: floatPtr(1.1),
					Retryer: &gcs.Retryer{
						BackoffInitial:    11 * time.Minute,
						BackoffMax:        111 * time.Minute,
						BackoffMultiplier: 11.1,
					},
				},
			},
		}
		err := SetConfig(ctx, connClient, &Config{Storage: StorageConf{Type: storage.GCS}})
		if err != nil {
			t.Fatal("set config:", err)
		}

		err = SetConfigVar(ctx, connClient,
			"storage.gcs.bucket",
			wantCfg.Storage.GCS.Bucket,
		)
		if err != nil {
			t.Fatal("set config var", err)
		}

		err = SetConfigVar(ctx, connClient,
			"storage.gcs.prefix",
			wantCfg.Storage.GCS.Prefix,
		)
		if err != nil {
			t.Fatal("set config var", err)
		}

		err = SetConfigVar(ctx, connClient,
			"storage.gcs.credentials.clientEmail",
			wantCfg.Storage.GCS.Credentials.ClientEmail,
		)
		if err != nil {
			t.Fatal("set config var", err)
		}

		err = SetConfigVar(ctx, connClient,
			"storage.gcs.credentials.privateKey",
			wantCfg.Storage.GCS.Credentials.PrivateKey,
		)
		if err != nil {
			t.Fatal("set config var", err)
		}

		err = SetConfigVar(ctx, connClient,
			"storage.gcs.chunkSize",
			fmt.Sprintf("%d", wantCfg.Storage.GCS.ChunkSize),
		)
		if err != nil {
			t.Fatal("set config var", err)
		}

		err = SetConfigVar(ctx, connClient,
			"storage.gcs.maxObjSizeGB",
			fmt.Sprintf("%f", *wantCfg.Storage.GCS.MaxObjSizeGB),
		)
		if err != nil {
			t.Fatal("set config var", err)
		}

		err = SetConfigVar(ctx, connClient,
			"storage.gcs.retryer.backoffInitial",
			wantCfg.Storage.GCS.Retryer.BackoffInitial.String(),
		)
		if err != nil {
			t.Fatal("set config var", err)
		}

		err = SetConfigVar(ctx, connClient,
			"storage.gcs.retryer.backoffMax",
			wantCfg.Storage.GCS.Retryer.BackoffMax.String(),
		)
		if err != nil {
			t.Fatal("set config var", err)
		}
		err = SetConfigVar(ctx, connClient,
			"storage.gcs.retryer.backoffMultiplier",
			fmt.Sprintf("%f", wantCfg.Storage.GCS.Retryer.BackoffMultiplier),
		)
		if err != nil {
			t.Fatal("set config var", err)
		}

		gotCfg, err := GetConfig(ctx, connClient)
		if err != nil {
			t.Fatal("get config:", err)
		}

		if !gotCfg.Storage.Equal(&wantCfg.Storage) {
			t.Fatalf("wrong config after using set config var, diff=%s",
				cmp.Diff(*wantCfg.Storage.GCS, *gotCfg.Storage.GCS))
		}
	})
}

func Test(t *testing.T) {
	testCases := []struct {
		desc string
	}{
		{
			desc: "",
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {

		})
	}
}

func boolPtr(b bool) *bool {
	return &b
}

func floatPtr(f float64) *float64 {
	return &f
}
