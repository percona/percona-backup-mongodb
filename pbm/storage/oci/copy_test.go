package oci

import (
	"context"
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
)

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
