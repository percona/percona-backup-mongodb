package oci

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func TestCastSetsDefaults(t *testing.T) {
	cfg := &Config{}

	require.NoError(t, cfg.Cast())

	assert.Equal(t, AuthTypeUserPrincipal, cfg.Credentials.Type)
	assert.Equal(t, defaultUploadPartSize, cfg.UploadPartSize)
	assert.Equal(t, defaultUploadConcurrency, cfg.UploadConcurrency)
	assert.Equal(t, float64(defaultMaxObjSizeGB), cfg.GetMaxObjSizeGB())
	assert.Empty(t, cfg.ServerSideEncryption.KmsKeyID)
	assert.Empty(t, cfg.ServerSideEncryption.SseCustomerKey)
	require.NotNil(t, cfg.Retryer)
	assert.Equal(t, defaultRetryMaxAttempts, cfg.Retryer.MaxAttempts)
	assert.Equal(t, defaultRetryMaxBackoff, cfg.Retryer.MaxBackoff)
}

func TestCastMissingConfig(t *testing.T) {
	var cfg *Config

	require.Error(t, cfg.Cast())
}

func TestCastRetryer(t *testing.T) {
	tests := []struct {
		name      string
		retryer   *Retryer
		want      Retryer
		wantError string
	}{
		{
			name:    "nil retryer",
			retryer: nil,
			want: Retryer{
				MaxAttempts: defaultRetryMaxAttempts,
				MaxBackoff:  defaultRetryMaxBackoff,
			},
		},
		{
			name: "partial retryer",
			retryer: &Retryer{
				MaxAttempts: 3,
			},
			want: Retryer{
				MaxAttempts: 3,
				MaxBackoff:  defaultRetryMaxBackoff,
			},
		},
		{
			name: "custom retryer",
			retryer: &Retryer{
				MaxAttempts: 4,
				MaxBackoff:  defaultRetryMaxBackoff * 2,
			},
			want: Retryer{
				MaxAttempts: 4,
				MaxBackoff:  defaultRetryMaxBackoff * 2,
			},
		},
		{
			name: "negative attempts",
			retryer: &Retryer{
				MaxAttempts: -1,
			},
			wantError: "retryer.maxAttempts cannot be negative",
		},
		{
			name: "negative backoff",
			retryer: &Retryer{
				MaxBackoff: -1,
			},
			wantError: "retryer.maxBackoff cannot be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{Retryer: tt.retryer}

			err := cfg.Cast()

			if tt.wantError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantError)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, cfg.Retryer)
			assert.Equal(t, tt.want, *cfg.Retryer)
		})
	}
}

func TestCastRetryerPreservesExistingPointer(t *testing.T) {
	retryer := &Retryer{MaxAttempts: 3}
	cfg := &Config{Retryer: retryer}

	require.NoError(t, cfg.Cast())

	assert.Same(t, retryer, cfg.Retryer)
	assert.Equal(t, 3, cfg.Retryer.MaxAttempts)
	assert.Equal(t, defaultRetryMaxBackoff, cfg.Retryer.MaxBackoff)
}

func TestCastObjectStorageLimits(t *testing.T) {
	tests := []struct {
		name      string
		cfg       *Config
		wantError string
	}{
		{
			name: "valid limits pass",
			cfg: &Config{
				UploadPartSize:    maxUploadPartSize,
				MaxObjSizeGB:      storage.Ref(float64(maxObjSizeGB) - 1),
				UploadConcurrency: maxUploadConcurrency,
			},
		},
		{
			name: "upload part size exceeds maximum",
			cfg: &Config{
				UploadPartSize: maxUploadPartSize + 1,
			},
			wantError: "uploadPartSize cannot exceed",
		},
		{
			name: "max object size reaches maximum",
			cfg: &Config{
				MaxObjSizeGB: storage.Ref(float64(maxObjSizeGB)),
			},
			wantError: "maxObjSizeGB must be less than",
		},
		{
			name: "upload concurrency exceeds maximum",
			cfg: &Config{
				UploadConcurrency: maxUploadConcurrency + 1,
			},
			wantError: "uploadConcurrency cannot exceed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Cast()

			if tt.wantError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantError)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestGetMaxObjSizeGB(t *testing.T) {
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
			name: "MaxObjSizeGB below upper bound returns configured value",
			cfg:  &Config{MaxObjSizeGB: storage.Ref(float64(maxObjSizeGB) - 1)},
			want: maxObjSizeGB - 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.GetMaxObjSizeGB()

			assert.Equal(t, tt.want, got)
		})
	}

	assert.Less(t, defaultMaxObjSizeGB, maxObjSizeGB)
}

func TestCloneCopiesUserPrincipal(t *testing.T) {
	cfg := testConfig(testPrivateKey(t))

	clone := cfg.Clone()
	require.NotNil(t, clone)
	require.NotNil(t, clone.Credentials.UserPrincipal)
	require.NotSame(t, cfg.Credentials.UserPrincipal, clone.Credentials.UserPrincipal)
	assert.Equal(t, cfg, clone)
}

func TestIsSameStorage(t *testing.T) {
	cfg := &Config{
		Region:    "eu-frankfurt-1",
		Namespace: "ns1",
		Bucket:    "b1",
		Prefix:    "p1",
		Credentials: Credentials{
			Type: AuthTypeUserPrincipal,
			UserPrincipal: &UserPrincipalCredentials{
				Tenancy:     "t1",
				User:        "u1",
				Fingerprint: "f1",
				PrivateKey:  "pk1",
			},
		},
		UploadPartSize:    1,
		UploadConcurrency: 2,
	}
	eq := &Config{
		Region:    cfg.Region,
		Namespace: cfg.Namespace,
		Bucket:    cfg.Bucket,
		Prefix:    cfg.Prefix,
	}

	require.True(t, cfg.IsSameStorage(eq))

	for name, mutate := range map[string]func(*Config){
		"region":    func(c *Config) { c.Region = "us-ashburn-1" },
		"namespace": func(c *Config) { c.Namespace = "ns2" },
		"bucket":    func(c *Config) { c.Bucket = "b2" },
		"prefix":    func(c *Config) { c.Prefix = "p2" },
	} {
		t.Run(name, func(t *testing.T) {
			neq := cfg.Clone()
			mutate(neq)
			assert.False(t, cfg.IsSameStorage(neq))
		})
	}
}

func TestConfigureClientMissingFields(t *testing.T) {
	privateKey := testPrivateKey(t)

	_, err := configureClient(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "config is nil")

	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{
			name:    "region",
			mutate:  func(cfg *Config) { cfg.Region = "" },
			wantErr: "region is required",
		},
		{
			name:    "namespace",
			mutate:  func(cfg *Config) { cfg.Namespace = "" },
			wantErr: "namespace is required",
		},
		{
			name:    "bucket",
			mutate:  func(cfg *Config) { cfg.Bucket = "" },
			wantErr: "bucket is required",
		},
		{
			name:    "unsupported credentials type",
			mutate:  func(cfg *Config) { cfg.Credentials.Type = "unknown" },
			wantErr: "unsupported OCI credentials type",
		},
		{
			name:    "user principal config",
			mutate:  func(cfg *Config) { cfg.Credentials.UserPrincipal = nil },
			wantErr: "credentials.userPrincipal is required",
		},
		{
			name:    "tenancy",
			mutate:  func(cfg *Config) { cfg.Credentials.UserPrincipal.Tenancy = "" },
			wantErr: "credentials.userPrincipal.tenancy is required",
		},
		{
			name:    "user",
			mutate:  func(cfg *Config) { cfg.Credentials.UserPrincipal.User = "" },
			wantErr: "credentials.userPrincipal.user is required",
		},
		{
			name:    "fingerprint",
			mutate:  func(cfg *Config) { cfg.Credentials.UserPrincipal.Fingerprint = "" },
			wantErr: "credentials.userPrincipal.fingerprint is required",
		},
		{
			name:    "private key",
			mutate:  func(cfg *Config) { cfg.Credentials.UserPrincipal.PrivateKey = "" },
			wantErr: "credentials.userPrincipal.privateKey is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig(privateKey)
			tt.mutate(cfg)

			_, err := configureClient(cfg)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestConfigureClient(t *testing.T) {
	cfg := testConfig(testPrivateKey(t))
	client, err := configureClient(cfg)

	require.NoError(t, err)
	require.NotNil(t, client)

	wantRetryPolicy := newRetryPolicy(cfg.Retryer)
	gotRetryPolicy := client.RetryPolicy()
	require.NotNil(t, gotRetryPolicy)
	assert.Equal(t, wantRetryPolicy.MaximumNumberAttempts, gotRetryPolicy.MaximumNumberAttempts)
	assert.Equal(t, wantRetryPolicy.MaxSleepBetween, gotRetryPolicy.MaxSleepBetween)
	assert.Equal(t, wantRetryPolicy.ExponentialBackoffBase, gotRetryPolicy.ExponentialBackoffBase)
	assert.Nil(t, gotRetryPolicy.NonEventuallyConsistentPolicy)
	require.NotNil(t, gotRetryPolicy.ShouldRetryOperation)
	require.NotNil(t, gotRetryPolicy.NextDuration)
}

func TestConfigureClientDefaultsToUserPrincipal(t *testing.T) {
	cfg := testConfig(testPrivateKey(t))
	cfg.Credentials.Type = ""

	client, err := configureClient(cfg)

	require.NoError(t, err)
	require.NotNil(t, client)
	require.NotNil(t, client.RetryPolicy())
}

func testConfig(privateKey string) *Config {
	return &Config{
		Region:    "eu-frankfurt-1",
		Namespace: "testns",
		Bucket:    "testbucket",
		Credentials: Credentials{
			Type: AuthTypeUserPrincipal,
			UserPrincipal: &UserPrincipalCredentials{
				Tenancy:     "ocid1.tenancy.oc1..test",
				User:        "ocid1.user.oc1..test",
				Fingerprint: "00:11:22:33:44:55:66:77:88:99:aa:bb:cc:dd:ee:ff",
				PrivateKey:  storage.MaskedString(privateKey),
			},
		},
	}
}

func testPrivateKey(t *testing.T) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}))
}
