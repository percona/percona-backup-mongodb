package oci

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func TestCastSetsDefaults(t *testing.T) {
	cfg := &Config{}

	require.NoError(t, cfg.Cast())

	assert.Equal(t, defaultUploadPartSize, cfg.UploadPartSize)
	assert.Equal(t, defaultMaxUploadParts, cfg.MaxUploadParts)
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

	_, _, err := configureClient(nil)
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

			_, _, err := configureClient(cfg)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestConfigureClient(t *testing.T) {
	client, retryPolicy, err := configureClient(testConfig(testPrivateKey(t)))

	require.NoError(t, err)
	require.NotNil(t, client)
	require.NotNil(t, retryPolicy)
	assert.Same(t, retryPolicy, client.RetryPolicy())
}

func TestCopyObjectRequest(t *testing.T) {
	o := &OCI{cfg: &Config{
		Region:    "eu-frankfurt-1",
		Namespace: "testns",
		Bucket:    "testbucket",
		Prefix:    "prefix",
	}}

	req := o.copyObjectRequest("src/file", "dst/file")

	require.NotNil(t, req.NamespaceName)
	assert.Equal(t, "testns", *req.NamespaceName)
	require.NotNil(t, req.BucketName)
	assert.Equal(t, "testbucket", *req.BucketName)
	require.NotNil(t, req.SourceObjectName)
	assert.Equal(t, "prefix/src/file", *req.SourceObjectName)
	require.NotNil(t, req.DestinationRegion)
	assert.Equal(t, "eu-frankfurt-1", *req.DestinationRegion)
	require.NotNil(t, req.DestinationNamespace)
	assert.Equal(t, "testns", *req.DestinationNamespace)
	require.NotNil(t, req.DestinationBucket)
	assert.Equal(t, "testbucket", *req.DestinationBucket)
	require.NotNil(t, req.DestinationObjectName)
	assert.Equal(t, "prefix/dst/file", *req.DestinationObjectName)
}

func TestCopyWorkRequestError(t *testing.T) {
	listErr := errors.New("list errors failed")

	tests := []struct {
		name     string
		err      *copyWorkRequestError
		contains []string
	}{
		{
			name: "with details",
			err: &copyWorkRequestError{
				id:     "wr1",
				status: objectstorage.WorkRequestStatusFailed,
				details: []objectstorage.WorkRequestError{
					{Code: common.String("NotFound"), Message: common.String("source missing")},
				},
			},
			contains: []string{"status FAILED", "NotFound: source missing"},
		},
		{
			name:     "without details",
			err:      &copyWorkRequestError{id: "wr1", status: objectstorage.WorkRequestStatusCanceled},
			contains: []string{"status CANCELED", "no error details returned"},
		},
		{
			name:     "list failed",
			err:      &copyWorkRequestError{id: "wr1", status: objectstorage.WorkRequestStatusFailed, listErr: listErr},
			contains: []string{"status FAILED", "failed to list error details", "list errors failed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertErrorContains(t, tt.err, "copy object work request wr1")
			assertErrorContains(t, tt.err, tt.contains...)
		})
	}

	assert.ErrorIs(t, &copyWorkRequestError{listErr: listErr}, listErr)
}

func TestWaitCopyWorkRequestContextDeadline(t *testing.T) {
	client, _, err := configureClient(testConfig(testPrivateKey(t)))
	require.NoError(t, err)

	httpClient := &inProgressWorkRequestHTTPClient{}
	client.HTTPClient = httpClient
	o := &OCI{client: client}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	err = o.waitCopyWorkRequest(ctx, "wr1")

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Positive(t, httpClient.calls)
}

func assertErrorContains(t *testing.T, err error, parts ...string) {
	t.Helper()

	msg := err.Error()
	for _, part := range parts {
		assert.Contains(t, msg, part)
	}
}

type inProgressWorkRequestHTTPClient struct {
	calls int
}

func (c *inProgressWorkRequestHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.calls++
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header: http.Header{
			"Retry-After": []string{"0.001"},
		},
		Body:    io.NopCloser(strings.NewReader(`{"id":"wr1","status":"IN_PROGRESS"}`)),
		Request: req,
	}, nil
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
