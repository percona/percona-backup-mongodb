package gcs

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func TestCast(t *testing.T) {
	var c *Config
	err := c.Cast()
	if err == nil {
		t.Fatal("sigsegv should have happened instead")
	}

	c = &Config{}
	err = c.Cast()
	if err != nil {
		t.Fatalf("got error during Cast: %v", err)
	}
	want := &Config{
		ClientType: ClientTypeJSON,
		ChunkSize:  defaultChunkSize,
		Retryer: &Retryer{
			MaxAttempts:        defaultMaxAttempts,
			BackoffInitial:     defaultBackoffInitial,
			BackoffMax:         defaultBackoffMax,
			BackoffMultiplier:  defaultBackoffMultiplier,
			ChunkRetryDeadline: defaultChunkRetryDeadline,
		},
	}

	if !c.Equal(want) {
		t.Fatalf("wrong config after Cast, diff=%s", cmp.Diff(*c, *want))
	}

	t.Run("parallel upload validation", func(t *testing.T) {
		for _, tt := range []struct {
			name string
			cfg  *Config
			err  string
		}{
			{
				name: "negative part size",
				cfg:  &Config{ParallelUpload: &ParallelUpload{PartSize: -1}},
				err:  "parallelUpload.partSize cannot be negative",
			},
			{
				name: "negative max concurrency",
				cfg:  &Config{ParallelUpload: &ParallelUpload{MaxConcurrency: -1}},
				err:  "parallelUpload.maxConcurrency cannot be negative",
			},
			{
				name: "invalid client type",
				cfg:  &Config{ClientType: ClientType("xml")},
				err:  `invalid clientType "xml"`,
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				err := tt.cfg.Cast()
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Fatalf("expected %q, got %v", tt.err, err)
				}
			})
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
		ClientType: ClientTypeGRPC,
		ParallelUpload: &ParallelUpload{
			Enabled:        true,
			PartSize:       16 * 1024 * 1024,
			MaxConcurrency: 4,
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

		opts.ParallelUpload.MaxConcurrency = 8
		if opts.Equal(clone) {
			t.Error("expected clone to be unchanged when updating original")
		}
		opts.ParallelUpload.MaxConcurrency = 4

		opts.Bucket = "updatedName"
		if opts.Equal(clone) {
			t.Error("expected clone to be unchanged when updating original")
		}
		opts.Bucket = clone.Bucket
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

		clone = opts.Clone()
		clone.ClientType = ClientTypeJSON
		if opts.Equal(clone) {
			t.Error("expected not to be equal when updating client type")
		}

		clone = opts.Clone()
		clone.ParallelUpload.Enabled = false
		if opts.Equal(clone) {
			t.Error("expected not to be equal when updating parallel upload")
		}
	})

	t.Run("GetMaxObjSizeGB", func(t *testing.T) {
		tests := []struct {
			name string
			cfg  *Config
			want float64
		}{
			{
				name: "nil MaxObjSizeGB returns default",
				cfg:  &Config{},
				want: defaultMaxObjSizeGB,
			},
			{
				name: "MaxObjSizeGB below lower bound returns default",
				cfg:  &Config{MaxObjSizeGB: storage.Ref(0.5)},
				want: defaultMaxObjSizeGB,
			},
			{
				name: "MaxObjSizeGB at lower bound returns configured value",
				cfg:  &Config{MaxObjSizeGB: storage.Ref(float64(storage.MinValidMaxObjSizeGB))},
				want: storage.MinValidMaxObjSizeGB,
			},
			{
				name: "MaxObjSizeGB above lower bound returns configured value",
				cfg:  &Config{MaxObjSizeGB: storage.Ref(float64(100))},
				want: 100,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := tt.cfg.GetMaxObjSizeGB()
				if got != tt.want {
					t.Errorf("GetMaxObjSizeGB: got=%v, want=%v", got, tt.want)
				}
			})
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
