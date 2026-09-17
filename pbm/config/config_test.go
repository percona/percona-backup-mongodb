package config

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/storage/azure"
	"github.com/percona/percona-backup-mongodb/pbm/storage/fs"
	"github.com/percona/percona-backup-mongodb/pbm/storage/gcs"
	"github.com/percona/percona-backup-mongodb/pbm/storage/mio"
	ocistorage "github.com/percona/percona-backup-mongodb/pbm/storage/oci"
	"github.com/percona/percona-backup-mongodb/pbm/storage/oss"
	"github.com/percona/percona-backup-mongodb/pbm/storage/s3"
)

func TestLifecycleConfDefaultsAndClone(t *testing.T) {
	cfg := &LifecycleConf{Strategy: "CALENDAR"}
	require.Equal(t, LifecycleStrategyCalendar, cfg.GetStrategy())
	require.Equal(t, DefaultLifecycleMinKeep, cfg.GetMinKeep())
	require.Nil(t, cfg.MinKeep)

	minKeep := DefaultLifecycleMinKeep
	cfg.MinKeep = &minKeep
	clone := cfg.Clone()
	require.NotSame(t, cfg, clone)
	require.NotSame(t, cfg.MinKeep, clone.MinKeep)
	*clone.MinKeep = 0
	assert.Equal(t, DefaultLifecycleMinKeep, *cfg.MinKeep)

	minKeep = 0
	cfg = &LifecycleConf{MinKeep: &minKeep}
	assert.Zero(t, cfg.GetMinKeep())

	clonedConfig := (&Config{Lifecycle: cfg}).Clone()
	require.NotSame(t, cfg.MinKeep, clonedConfig.Lifecycle.MinKeep)
}

func TestValidateLifecycle(t *testing.T) {
	negative := -1
	tests := []struct {
		name    string
		cfg     LifecycleConf
		wantErr string
	}{
		{name: "defaults"},
		{name: "rolling", cfg: LifecycleConf{Strategy: LifecycleStrategyRolling}},
		{
			name: "calendar",
			cfg: LifecycleConf{
				Strategy:         LifecycleStrategyCalendar,
				WeeklyRetention:  1,
				WeeklyDay:        int(time.Saturday),
				MonthlyRetention: 1,
				MonthlyDay:       31,
			},
		},
		{name: "unknown strategy", cfg: LifecycleConf{Strategy: "unknown"}, wantErr: "lifecycle.strategy"},
		{name: "negative daily", cfg: LifecycleConf{DailyRetention: -1}, wantErr: "lifecycle.dailyRetention"},
		{name: "negative weekly", cfg: LifecycleConf{WeeklyRetention: -1}, wantErr: "lifecycle.weeklyRetention"},
		{name: "negative monthly", cfg: LifecycleConf{MonthlyRetention: -1}, wantErr: "lifecycle.monthlyRetention"},
		{name: "negative minKeep", cfg: LifecycleConf{MinKeep: &negative}, wantErr: "lifecycle.minKeep"},
		{
			name: "invalid weekly day",
			cfg: LifecycleConf{
				Strategy:        LifecycleStrategyCalendar,
				WeeklyRetention: 1,
				WeeklyDay:       7,
			},
			wantErr: "lifecycle.weeklyDay",
		},
		{
			name: "invalid monthly day",
			cfg: LifecycleConf{
				Strategy:         LifecycleStrategyCalendar,
				MonthlyRetention: 1,
			},
			wantErr: "lifecycle.monthlyDay",
		},
		{
			name: "rolling ignores target days",
			cfg: LifecycleConf{
				Strategy:         LifecycleStrategyRolling,
				WeeklyRetention:  1,
				WeeklyDay:        7,
				MonthlyRetention: 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateLifecycle(&tt.cfg)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestParseValidatesLifecycle(t *testing.T) {
	_, err := Parse(strings.NewReader(`
storage:
  type: blackhole
lifecycle:
  strategy: unknown
`))
	require.ErrorContains(t, err, "lifecycle.strategy")
}

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
			Region:      "eu",
			Bucket:      "b1",
			Prefix:      "p1",
			EndpointURL: "ep.com",
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

		neq = cfg.Clone()
		neq.EndpointURL = "ep2.com"
		if cfg.IsSameStorage(neq) {
			t.Errorf("storage instances has different EndpointURL: cfg=%+v, eq=%+v", cfg, neq)
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
			Account:     "a1",
			Container:   "c1",
			Prefix:      "p1",
			EndpointURL: "az.com",
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

		neq = cfg.Clone()
		neq.EndpointURL = "az2.com"
		if cfg.IsSameStorage(neq) {
			t.Errorf("storage instances has different EndpointURL: cfg=%+v, eq=%+v", cfg, neq)
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

	t.Run("oss", func(t *testing.T) {
		cfg := &oss.Config{
			Region:      "eu",
			EndpointURL: "ep.com",
			Bucket:      "b1",
			Prefix:      "p1",
			Credentials: oss.Credentials{
				AccessKeyID:     "k1",
				AccessKeySecret: "k2",
				SecurityToken:   "sect",
			},
			ConnectTimeout:       10 * time.Second,
			UploadPartSize:       6 << 20,
			MaxObjSizeGB:         floatPtr(1.1),
			Retryer:              &oss.Retryer{},
			ServerSideEncryption: &oss.SSE{},
		}
		eq := &oss.Config{
			Region:      "eu",
			EndpointURL: "ep.com",
			Bucket:      "b1",
			Prefix:      "p1",
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
		neq.EndpointURL = "ep2.com"
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

	t.Run("OCI", func(t *testing.T) {
		cfg := &ocistorage.Config{
			Region:    "eu-frankfurt-1",
			Namespace: "ns1",
			Bucket:    "b1",
			Prefix:    "p1",
			Credentials: ocistorage.Credentials{
				Type: ocistorage.AuthTypeUserPrincipal,
				UserPrincipal: &ocistorage.UserPrincipalCredentials{
					Tenancy:     "t1",
					User:        "u1",
					Fingerprint: "f1",
					PrivateKey:  "pk1",
				},
			},
		}
		eq := &ocistorage.Config{
			Region:    "eu-frankfurt-1",
			Namespace: "ns1",
			Bucket:    "b1",
			Prefix:    "p1",
		}
		if !cfg.IsSameStorage(eq) {
			t.Errorf("config storage should identify the same instance: cfg=%+v, eq=%+v", cfg, eq)
		}

		neq := cfg.Clone()
		neq.Region = "us-ashburn-1"
		if cfg.IsSameStorage(neq) {
			t.Errorf("storage instances has different region: cfg=%+v, eq=%+v", cfg, neq)
		}

		neq = cfg.Clone()
		neq.Namespace = "ns2"
		if cfg.IsSameStorage(neq) {
			t.Errorf("storage instances has different namespace: cfg=%+v, eq=%+v", cfg, neq)
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
	mClient, err := mongo.Connect(options.Client().ApplyURI(connStr))
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
					ClientType:                gcs.ClientTypeJSON,
					ChunkSize:                 100,
					ParallelUploadConcurrency: 4,
					MaxObjSizeGB:              floatPtr(1.1),
					Retryer: &gcs.Retryer{
						BackoffInitial:     11 * time.Minute,
						BackoffMax:         111 * time.Minute,
						BackoffMultiplier:  11.1,
						MaxAttempts:        1,
						ChunkRetryDeadline: 11 * time.Millisecond,
					},
				},
			},
		}

		testCases := []struct {
			desc  string
			param string
			val   string
		}{
			{
				desc:  "bucket",
				param: "storage.gcs.bucket",
				val:   wantCfg.Storage.GCS.Bucket,
			},
			{
				desc:  "prefix",
				param: "storage.gcs.prefix",
				val:   wantCfg.Storage.GCS.Prefix,
			},
			{
				desc:  "credentials.clientEmail",
				param: "storage.gcs.credentials.clientEmail",
				val:   string(wantCfg.Storage.GCS.Credentials.ClientEmail),
			},
			{
				desc:  "credentials.privateKey",
				param: "storage.gcs.credentials.privateKey",
				val:   string(wantCfg.Storage.GCS.Credentials.PrivateKey),
			},
			{
				desc:  "chunkSize",
				param: "storage.gcs.chunkSize",
				val:   fmt.Sprintf("%d", wantCfg.Storage.GCS.ChunkSize),
			},
			{
				desc:  "parallelUploadConcurrency",
				param: "storage.gcs.parallelUploadConcurrency",
				val:   fmt.Sprintf("%d", wantCfg.Storage.GCS.ParallelUploadConcurrency),
			},
			{
				desc:  "maxObjSizeGB",
				param: "storage.gcs.maxObjSizeGB",
				val:   fmt.Sprintf("%f", *wantCfg.Storage.GCS.MaxObjSizeGB),
			},
			{
				desc:  "retryer.backoffInitial",
				param: "storage.gcs.retryer.backoffInitial",
				val:   wantCfg.Storage.GCS.Retryer.BackoffInitial.String(),
			},
			{
				desc:  "retryer.backoffMax",
				param: "storage.gcs.retryer.backoffMax",
				val:   wantCfg.Storage.GCS.Retryer.BackoffMax.String(),
			},
			{
				desc:  "retryer.backoffMultiplier",
				param: "storage.gcs.retryer.backoffMultiplier",
				val:   fmt.Sprintf("%f", wantCfg.Storage.GCS.Retryer.BackoffMultiplier),
			},
			{
				desc:  "retryer.maxAttempts",
				param: "storage.gcs.retryer.maxAttempts",
				val:   fmt.Sprintf("%d", wantCfg.Storage.GCS.Retryer.MaxAttempts),
			},
			{
				desc:  "retryer.chunkRetryDeadline",
				param: "storage.gcs.retryer.chunkRetryDeadline",
				val:   wantCfg.Storage.GCS.Retryer.ChunkRetryDeadline.String(),
			},
		}

		emptyCfg := &Config{
			Storage: StorageConf{Type: storage.GCS, GCS: &gcs.Config{}},
		}
		err := SetConfig(ctx, connClient, emptyCfg)
		if err != nil {
			t.Fatalf("setup: initial SetConfig failed: %v", err)
		}

		for _, tt := range testCases {
			t.Run(tt.desc, func(t *testing.T) {
				err := SetConfigVar(ctx, connClient, tt.param, tt.val)
				if err != nil {
					t.Fatalf("SetConfigVar failed for %s with value %s: %v",
						tt.param, tt.val, err)
				}
			})
		}

		t.Run("check final config", func(t *testing.T) {
			gotCfg, err := GetConfig(ctx, connClient)
			if err != nil {
				t.Fatalf("GetConfig failed: %v", err)
			}

			if !gotCfg.Storage.Equal(&wantCfg.Storage) {
				t.Fatalf("Wrong config after using SetConfigVar.\n-want: %+v\n-got: %+v\n\nDiff:\n%s",
					wantCfg.Storage.GCS, gotCfg.Storage.GCS, cmp.Diff(*wantCfg.Storage.GCS, *gotCfg.Storage.GCS))
			}
		})
	})

	t.Run("gcs parallel upload config", func(t *testing.T) {
		emptyCfg := &Config{
			Storage: StorageConf{Type: storage.GCS, GCS: &gcs.Config{}},
		}
		err := SetConfig(ctx, connClient, emptyCfg)
		if err != nil {
			t.Fatalf("setup: initial SetConfig failed: %v", err)
		}

		testCases := []struct {
			desc  string
			param string
			val   string
		}{
			{
				desc:  "clientType",
				param: "storage.gcs.clientType",
				val:   string(gcs.ClientTypeGRPC),
			},
			{
				desc:  "parallelUploadConcurrency",
				param: "storage.gcs.parallelUploadConcurrency",
				val:   "4",
			},
		}

		for _, tt := range testCases {
			t.Run(tt.desc, func(t *testing.T) {
				err := SetConfigVar(ctx, connClient, tt.param, tt.val)
				if err != nil {
					t.Fatalf("SetConfigVar failed for %s with value %s: %v",
						tt.param, tt.val, err)
				}
			})
		}

		gotCfg, err := GetConfig(ctx, connClient)
		if err != nil {
			t.Fatalf("GetConfig failed: %v", err)
		}

		gotGCS := gotCfg.Storage.GCS
		if gotGCS.ClientType != gcs.ClientTypeGRPC {
			t.Fatalf("clientType: got=%q, want=%q", gotGCS.ClientType, gcs.ClientTypeGRPC)
		}
		if gotGCS.ParallelUploadConcurrency != 4 {
			t.Fatalf("parallelUploadConcurrency: got=%d, want=4", gotGCS.ParallelUploadConcurrency)
		}
	})

	t.Run("restore config", func(t *testing.T) {
		emptyCfg := &Config{
			Storage: StorageConf{Type: storage.Blackhole},
		}
		err := SetConfig(ctx, connClient, emptyCfg)
		require.NoError(t, err)

		testCases := []struct {
			desc  string
			param string
			val   string
			check func(t *testing.T, cfg *Config)
		}{
			{
				desc:  "indexCommitQuorum",
				param: "restore.indexCommitQuorum",
				val:   string(IndexCommitQuorumMajority),
				check: func(t *testing.T, cfg *Config) {
					t.Helper()
					require.NotNil(t, cfg.Restore)
					assert.Equal(t, IndexCommitQuorumMajority, cfg.Restore.IndexCommitQuorum)
				},
			},
		}

		for _, tt := range testCases {
			t.Run(tt.desc, func(t *testing.T) {
				err := SetConfigVar(ctx, connClient, tt.param, tt.val)
				require.NoError(t, err)

				got, err := GetConfigVar(ctx, connClient, tt.param)
				require.NoError(t, err)
				assert.Equal(t, tt.val, got)

				gotCfg, err := GetConfig(ctx, connClient)
				require.NoError(t, err)
				tt.check(t, gotCfg)
			})
		}

		err = SetConfigVar(ctx, connClient, "restore.indexCommitQuorum", "whatever")
		require.Error(t, err)

		err = SetConfig(ctx, connClient, &Config{
			Storage: StorageConf{Type: storage.Blackhole},
			Restore: &RestoreConf{IndexCommitQuorum: "0"},
		})
		require.Error(t, err)
	})
}

func TestS3DebugLogLevelValidation(t *testing.T) {
	ctx := context.Background()
	newConfig := func(levels string) *Config {
		return &Config{
			Storage: StorageConf{
				Type: storage.S3,
				S3: &s3.Config{
					Bucket:         "bucket",
					DebugLogLevels: levels,
				},
			},
		}
	}

	const validLevels = "Signing,Retries"
	require.NoError(t, SetConfig(ctx, connClient, newConfig(validLevels)))
	require.NoError(t, SetConfigVar(ctx, connClient, "storage.s3.debugLogLevels", "Request,Response"))

	err := SetConfigVar(ctx, connClient, "storage.s3.debugLogLevels", "RequestEventMessage")
	require.ErrorContains(t, err, "set s3 debug log")

	err = SetConfig(ctx, connClient, newConfig("LogDebug"))
	require.Error(t, err)

	profile := newConfig("Unknown")
	profile.Name = "invalid-debug-log-level"
	profile.IsProfile = true
	err = AddProfile(ctx, connClient, profile)
	require.Error(t, err)

	_, err = connClient.ConfigCollection().UpdateOne(ctx,
		bson.D{{"profile", nil}},
		bson.M{"$set": bson.M{"storage.s3.debugLogLevels": "Unknown"}},
	)
	require.NoError(t, err)

	persisted, err := GetConfig(ctx, connClient)
	require.NoError(t, err)
	require.ErrorContains(t, persisted.Storage.Cast(), "validate s3 debug log")

	require.NoError(t, SetConfigVar(ctx, connClient, "storage.s3.debugLogLevels", "Signing"))
	got, err := GetConfigVar(ctx, connClient, "storage.s3.debugLogLevels")
	require.NoError(t, err)
	assert.Equal(t, "Signing", got)
}

func TestLifecycleConfigPersistenceValidation(t *testing.T) {
	ctx := context.Background()
	minKeep := 0
	cfg := &Config{
		Storage: StorageConf{Type: storage.Blackhole},
		Lifecycle: &LifecycleConf{
			Enabled:        true,
			Strategy:       "ROLLING",
			MinKeep:        &minKeep,
			DailyRetention: 7,
		},
	}
	require.NoError(t, SetConfig(ctx, connClient, cfg))
	assert.Equal(t, "ROLLING", cfg.Lifecycle.Strategy)

	invalid := cfg.Clone()
	invalid.Lifecycle.DailyRetention = -1
	require.ErrorContains(t, SetConfig(ctx, connClient, invalid), "lifecycle.dailyRetention")

	persisted, err := GetConfig(ctx, connClient)
	require.NoError(t, err)
	assert.Equal(t, 7, persisted.Lifecycle.DailyRetention)
	assert.Zero(t, *persisted.Lifecycle.MinKeep)

	require.NoError(t, SetConfigVar(ctx, connClient, "lifecycle.strategy", "CALENDAR"))
	strategy, err := GetConfigVar(ctx, connClient, "lifecycle.strategy")
	require.NoError(t, err)
	assert.Equal(t, "CALENDAR", strategy)

	require.ErrorContains(t,
		SetConfigVar(ctx, connClient, "lifecycle.minKeep", "-1"),
		"lifecycle.minKeep",
	)
	persisted, err = GetConfig(ctx, connClient)
	require.NoError(t, err)
	assert.Zero(t, *persisted.Lifecycle.MinKeep)

	const profileName = "lifecycle-validation"
	t.Cleanup(func() {
		_ = RemoveProfile(context.Background(), connClient, profileName)
	})
	profile := &Config{
		Name:      profileName,
		IsProfile: true,
		Storage:   StorageConf{Type: storage.Blackhole},
		Lifecycle: &LifecycleConf{
			MinKeep:        &minKeep,
			DailyRetention: 5,
		},
	}
	require.NoError(t, AddProfile(ctx, connClient, profile))

	invalidProfile := profile.Clone()
	invalidProfile.Lifecycle.Strategy = "unknown"
	require.ErrorContains(t, AddProfile(ctx, connClient, invalidProfile), "lifecycle.strategy")

	persistedProfile, err := GetProfile(ctx, connClient, profileName)
	require.NoError(t, err)
	assert.Nil(t, persistedProfile.PITR)
	assert.Nil(t, persistedProfile.Backup)
	assert.Nil(t, persistedProfile.Restore)
	require.NotNil(t, persistedProfile.Lifecycle)
	assert.Equal(t, LifecycleStrategyRolling, persistedProfile.Lifecycle.GetStrategy())
	assert.Equal(t, 5, persistedProfile.Lifecycle.DailyRetention)
	assert.Zero(t, *persistedProfile.Lifecycle.MinKeep)
}

func TestRestoreConfGetIndexCommitQuorum(t *testing.T) {
	tests := []struct {
		name string
		cfg  *RestoreConf
		want IndexCommitQuorum
	}{
		{name: "nil config", cfg: nil, want: DefaultRestoreIndexCommitQuorum},
		{name: "empty value", cfg: &RestoreConf{}, want: DefaultRestoreIndexCommitQuorum},
		{
			name: "configured string value",
			cfg:  &RestoreConf{IndexCommitQuorum: IndexCommitQuorumVotingMembers},
			want: IndexCommitQuorumVotingMembers,
		},
		{name: "configured numeric value", cfg: &RestoreConf{IndexCommitQuorum: "3"}, want: "3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.cfg.GetIndexCommitQuorum())
		})
	}
}

func TestParseRestoreIndexCommitQuorum(t *testing.T) {
	cfg, err := Parse(strings.NewReader(`
storage:
  type: blackhole
restore:
  indexCommitQuorum: votingMembers
`))
	require.NoError(t, err)
	require.NotNil(t, cfg.Restore)
	assert.Equal(t, IndexCommitQuorumVotingMembers, cfg.Restore.IndexCommitQuorum)
}

func TestSanitizeStoragePaths(t *testing.T) {
	tests := []struct {
		name       string
		conf       StorageConf
		wantBucket string
		wantPrefix string
	}{
		{
			"s3 trailing slash on bucket",
			StorageConf{Type: storage.S3, S3: &s3.Config{Bucket: "bcp/", Prefix: "data"}},
			"bcp", "data",
		},
		{
			"s3 leading slash on prefix",
			StorageConf{Type: storage.S3, S3: &s3.Config{Bucket: "bcp", Prefix: "/data/pbm"}},
			"bcp", "data/pbm",
		},
		{
			"s3 both slashes",
			StorageConf{Type: storage.S3, S3: &s3.Config{Bucket: "bcp/", Prefix: "/data/"}},
			"bcp", "data",
		},
		{
			"s3 multiple slashes",
			StorageConf{Type: storage.S3, S3: &s3.Config{Bucket: "///bcp///", Prefix: "///data///"}},
			"bcp", "data",
		},
		{
			"s3 clean values",
			StorageConf{Type: storage.S3, S3: &s3.Config{Bucket: "bcp", Prefix: "data"}},
			"bcp", "data",
		},
		{
			"minio trailing slash",
			StorageConf{Type: storage.Minio, Minio: &mio.Config{Bucket: "bcp/", Prefix: "/pfx/"}},
			"bcp", "pfx",
		},
		{
			"gcs leading slash on prefix",
			StorageConf{Type: storage.GCS, GCS: &gcs.Config{Bucket: "bcp/", Prefix: "/pfx"}},
			"bcp", "pfx",
		},
		{
			"oss trailing slash",
			StorageConf{Type: storage.OSS, OSS: &oss.Config{Bucket: "bcp/", Prefix: "/pfx/"}},
			"bcp", "pfx",
		},
		{
			"oci trailing slash",
			StorageConf{Type: storage.OCI, OCI: &ocistorage.Config{Bucket: "bcp/", Prefix: "/pfx/"}},
			"bcp", "pfx",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sanitizeStoragePaths(&tt.conf)
			switch tt.conf.Type {
			case storage.S3:
				assert.Equal(t, tt.wantBucket, tt.conf.S3.Bucket)
				assert.Equal(t, tt.wantPrefix, tt.conf.S3.Prefix)
			case storage.Minio:
				assert.Equal(t, tt.wantBucket, tt.conf.Minio.Bucket)
				assert.Equal(t, tt.wantPrefix, tt.conf.Minio.Prefix)
			case storage.GCS:
				assert.Equal(t, tt.wantBucket, tt.conf.GCS.Bucket)
				assert.Equal(t, tt.wantPrefix, tt.conf.GCS.Prefix)
			case storage.OSS:
				assert.Equal(t, tt.wantBucket, tt.conf.OSS.Bucket)
				assert.Equal(t, tt.wantPrefix, tt.conf.OSS.Prefix)
			case storage.OCI:
				assert.Equal(t, tt.wantBucket, tt.conf.OCI.Bucket)
				assert.Equal(t, tt.wantPrefix, tt.conf.OCI.Prefix)
			}
		})
	}
}

func TestSanitizeStoragePathsAzure(t *testing.T) {
	conf := StorageConf{Type: storage.Azure, Azure: &azure.Config{Container: "cnt/", Prefix: "/pfx/"}}
	sanitizeStoragePaths(&conf)
	assert.Equal(t, "cnt", conf.Azure.Container)
	assert.Equal(t, "pfx", conf.Azure.Prefix)
}

func TestIsStoragePathKey(t *testing.T) {
	// Storage keys that should match.
	for _, key := range []string{
		"storage.s3.bucket",
		"storage.s3.prefix",
		"storage.minio.bucket",
		"storage.minio.prefix",
		"storage.gcs.bucket",
		"storage.gcs.prefix",
		"storage.azure.container",
		"storage.azure.prefix",
		"storage.oss.bucket",
		"storage.oss.prefix",
		"storage.oci.bucket",
		"storage.oci.prefix",
	} {
		assert.True(t, isStoragePathKey(key), "expected true for %q", key)
	}

	// Non-storage keys must not match.
	for _, key := range []string{
		"pitr.enabled",
		"pitr.compression",
		"storage.s3.region",
		"storage.s3.debugLogLevels",
		"storage.filesystem.path",
		"bucket",
		"prefix",
	} {
		assert.False(t, isStoragePathKey(key), "expected false for %q", key)
	}
}

func TestSetConfigVarTrimsSlashes(t *testing.T) {
	ctx := context.Background()

	// Set up initial config so SetConfigVar works
	emptyCfg := &Config{
		Storage: StorageConf{Type: storage.S3, S3: &s3.Config{Bucket: "init"}},
	}
	err := SetConfig(ctx, connClient, emptyCfg)
	require.NoError(t, err)

	tests := []struct {
		key     string
		val     string
		wantVal string
	}{
		{"storage.s3.bucket", "bcp/", "bcp"},
		{"storage.s3.prefix", "/data/pbm/", "data/pbm"},
		{"storage.s3.bucket", "///bcp///", "bcp"},
		{"storage.s3.prefix", "clean", "clean"},
	}

	for _, tt := range tests {
		t.Run(tt.key+"="+tt.val, func(t *testing.T) {
			err := SetConfigVar(ctx, connClient, tt.key, tt.val)
			require.NoError(t, err)

			got, err := GetConfigVar(ctx, connClient, tt.key)
			require.NoError(t, err)
			assert.Equal(t, tt.wantVal, got)
		})
	}
}

func TestBackupConfGetNumParallelFiles(t *testing.T) {
	tests := []struct {
		name string
		cfg  *BackupConf
		want int
	}{
		{name: "nil receiver", cfg: nil, want: 1},
		{name: "unset (zero) defaults to 1", cfg: &BackupConf{}, want: 1},
		{name: "negative defaults to 1", cfg: &BackupConf{NumParallelFiles: -5}, want: 1},
		{name: "one", cfg: &BackupConf{NumParallelFiles: 1}, want: 1},
		{name: "configured value", cfg: &BackupConf{NumParallelFiles: 8}, want: 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.GetNumParallelFiles(); got != tt.want {
				t.Errorf("GetNumParallelFiles() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestRestoreConfGetNumParallelFiles(t *testing.T) {
	tests := []struct {
		name string
		cfg  *RestoreConf
		want int
	}{
		{name: "nil receiver", cfg: nil, want: 1},
		{name: "unset (zero) defaults to 1", cfg: &RestoreConf{}, want: 1},
		{name: "negative defaults to 1", cfg: &RestoreConf{NumParallelFiles: -5}, want: 1},
		{name: "one", cfg: &RestoreConf{NumParallelFiles: 1}, want: 1},
		{name: "configured value", cfg: &RestoreConf{NumParallelFiles: 8}, want: 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.GetNumParallelFiles(); got != tt.want {
				t.Errorf("GetNumParallelFiles() = %d, want %d", got, tt.want)
			}
		})
	}
}

func boolPtr(b bool) *bool {
	return &b
}

func floatPtr(f float64) *float64 {
	return &f
}
