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

	assert.Equal(t, defaultUploadPartSize, cfg.UploadPartSize)
	assert.Equal(t, defaultMaxUploadParts, cfg.MaxUploadParts)
}

func TestCastMissingConfig(t *testing.T) {
	var cfg *Config

	require.Error(t, cfg.Cast())
}

func TestIsSameStorage(t *testing.T) {
	cfg := &Config{
		Region:    "eu-frankfurt-1",
		Namespace: "ns1",
		Bucket:    "b1",
		Prefix:    "p1",
		Credentials: Credentials{
			Tenancy:     "t1",
			User:        "u1",
			Fingerprint: "f1",
			PrivateKey:  "pk1",
		},
		UploadPartSize: 1,
		MaxUploadParts: 2,
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
			name:    "tenancy",
			mutate:  func(cfg *Config) { cfg.Credentials.Tenancy = "" },
			wantErr: "credentials.tenancy is required",
		},
		{
			name:    "user",
			mutate:  func(cfg *Config) { cfg.Credentials.User = "" },
			wantErr: "credentials.user is required",
		},
		{
			name:    "fingerprint",
			mutate:  func(cfg *Config) { cfg.Credentials.Fingerprint = "" },
			wantErr: "credentials.fingerprint is required",
		},
		{
			name:    "private key",
			mutate:  func(cfg *Config) { cfg.Credentials.PrivateKey = "" },
			wantErr: "credentials.privateKey is required",
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
	client, err := configureClient(testConfig(testPrivateKey(t)))

	require.NoError(t, err)
	require.NotNil(t, client)
}

func testConfig(privateKey string) *Config {
	return &Config{
		Region:    "eu-frankfurt-1",
		Namespace: "testns",
		Bucket:    "testbucket",
		Credentials: Credentials{
			Tenancy:     "ocid1.tenancy.oc1..test",
			User:        "ocid1.user.oc1..test",
			Fingerprint: "00:11:22:33:44:55:66:77:88:99:aa:bb:cc:dd:ee:ff",
			PrivateKey:  storage.MaskedString(privateKey),
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
