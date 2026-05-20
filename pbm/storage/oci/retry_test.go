package oci

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRetryPolicy(t *testing.T) {
	tests := []struct {
		name        string
		retryer     *Retryer
		wantAttempt uint
		wantBackoff float64
	}{
		{
			name:        "nil retryer",
			retryer:     nil,
			wantAttempt: uint(defaultRetryMaxAttempts),
			wantBackoff: defaultRetryMaxBackoff.Seconds(),
		},
		{
			name:        "zero retryer",
			retryer:     &Retryer{},
			wantAttempt: uint(defaultRetryMaxAttempts),
			wantBackoff: defaultRetryMaxBackoff.Seconds(),
		},
		{
			name: "custom attempts",
			retryer: &Retryer{
				MaxAttempts: 4,
			},
			wantAttempt: 4,
			wantBackoff: defaultRetryMaxBackoff.Seconds(),
		},
		{
			name: "custom backoff",
			retryer: &Retryer{
				MaxBackoff: defaultRetryMaxBackoff * 2,
			},
			wantAttempt: uint(defaultRetryMaxAttempts),
			wantBackoff: (defaultRetryMaxBackoff * 2).Seconds(),
		},
		{
			name: "disable retries",
			retryer: &Retryer{
				MaxAttempts: 1,
			},
			wantAttempt: 1,
			wantBackoff: defaultRetryMaxBackoff.Seconds(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := newRetryPolicy(tt.retryer)

			require.NotNil(t, policy)
			assert.Equal(t, tt.wantAttempt, policy.MaximumNumberAttempts)
			assert.Equal(t, tt.wantBackoff, policy.MaxSleepBetween)
			assert.Equal(t, retryBackoffBase, policy.ExponentialBackoffBase)
			assert.Nil(t, policy.NonEventuallyConsistentPolicy)
			require.NotNil(t, policy.ShouldRetryOperation)
			require.NotNil(t, policy.NextDuration)
		})
	}
}

func TestRetryPolicyRetriesDirectRequests(t *testing.T) {
	const (
		maxAttempts = 3
		objectSize  = int64(42)
	)

	cfg := testConfig(testPrivateKey(t))
	cfg.Retryer = &Retryer{
		MaxAttempts: maxAttempts,
		MaxBackoff:  time.Second,
	}
	client, err := configureClient(cfg)
	require.NoError(t, err)
	retryPolicy := client.RetryPolicy()
	require.NotNil(t, retryPolicy)

	// Avoid sleeping between retries; this test only verifies retry attempts.
	retryPolicy.NextDuration = func(common.OCIOperationResponse) time.Duration { return 0 }

	httpClient := testHTTPClient(maxAttempts, objectSize)
	client.HTTPClient = httpClient

	o := &OCI{cfg: cfg, client: client}
	inf, err := o.FileStat("test.dat")

	require.NoError(t, err)
	assert.Equal(t, maxAttempts, httpClient.calls)
	assert.Equal(t, objectSize, inf.Size)
}

type retryTestHTTPClient struct {
	maxAttempts int
	objectSize  int64
	calls       int
}

func testHTTPClient(maxAttempts int, objectSize int64) *retryTestHTTPClient {
	return &retryTestHTTPClient{maxAttempts: maxAttempts, objectSize: objectSize}
}

func (c *retryTestHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.calls++
	if c.calls < c.maxAttempts {
		return ociInternalServerError(req), nil
	}

	return headObjectResponse(req, c.objectSize), nil
}

func ociInternalServerError(req *http.Request) *http.Response {
	return &http.Response{
		StatusCode: http.StatusInternalServerError,
		Status:     "500 Internal Server Error",
		Header:     make(http.Header),
		Body:       http.NoBody,
		Request:    req,
	}
}

func headObjectResponse(req *http.Request, size int64) *http.Response {
	header := make(http.Header)
	header.Set("Content-Length", strconv.FormatInt(size, 10))

	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     header,
		Body:       http.NoBody,
		Request:    req,
	}
}
