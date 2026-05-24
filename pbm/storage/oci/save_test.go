package oci

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func TestCheckEmptyReader(t *testing.T) {
	tests := []struct {
		name      string
		reader    io.Reader
		wantEmpty bool
		wantData  string
	}{
		{
			name:      "empty reader",
			reader:    strings.NewReader(""),
			wantEmpty: true,
		},
		{
			name:      "empty limited reader",
			reader:    io.LimitReader(bytes.NewReader(nil), 0),
			wantEmpty: true,
		},
		{
			name:     "non-empty reader",
			reader:   strings.NewReader("data"),
			wantData: "data",
		},
		{
			name:     "one byte reader with eof",
			reader:   &oneByteEOFReader{},
			wantData: "x",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			empty, r, err := checkEmptyReader(tt.reader)

			require.NoError(t, err)
			assert.Equal(t, tt.wantEmpty, empty)
			if !tt.wantEmpty {
				data, err := io.ReadAll(r)
				require.NoError(t, err)
				assert.Equal(t, tt.wantData, string(data))
			}
		})
	}
}

func TestCheckEmptyReaderFirstByteError(t *testing.T) {
	empty, _, err := checkEmptyReader(&oneByteErrorReader{})

	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	assert.False(t, empty)
}

func TestCheckEmptyReaderNoProgress(t *testing.T) {
	empty, _, err := checkEmptyReader(zeroReader{})

	require.ErrorIs(t, err, io.ErrNoProgress)
	assert.False(t, empty)
}

type oneByteEOFReader struct {
	done bool
}

func (r *oneByteEOFReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	p[0] = 'x'
	return 1, io.EOF
}

type oneByteErrorReader struct{}

func (*oneByteErrorReader) Read(p []byte) (int, error) {
	p[0] = 'x'
	return 1, io.ErrUnexpectedEOF
}

type zeroReader struct{}

func (zeroReader) Read([]byte) (int, error) {
	return 0, nil
}

// TestSaveEmptyObjectWorkaround validates the OCI SDK empty-stream workaround:
// Save must bypass transfer.UploadStream for empty readers regardless of size hint.
func TestSaveEmptyObjectWorkaround(t *testing.T) {
	tests := []struct {
		name    string
		reader  io.Reader
		options []storage.Option
	}{
		{
			name:    "known empty object",
			reader:  strings.NewReader(""),
			options: []storage.Option{storage.Size(0)},
		},
		{
			name:   "unknown size empty object",
			reader: io.LimitReader(bytes.NewReader(nil), 0),
		},
		{
			name:    "empty object with positive size hint",
			reader:  io.LimitReader(bytes.NewReader(nil), 0),
			options: []storage.Option{storage.Size(1024)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := configureClient(testConfig(testPrivateKey(t)))
			require.NoError(t, err)
			httpClient := &emptyPutHTTPClient{}
			client.HTTPClient = httpClient
			o := &OCI{cfg: &Config{
				Region:    "eu-frankfurt-1",
				Namespace: "testns",
				Bucket:    "testbucket",
				Prefix:    "prefix",
			}, client: client}

			err = o.Save("file", tt.reader, tt.options...)

			require.NoError(t, err)
			assert.Equal(t, 1, httpClient.calls)
			assert.Equal(t, http.MethodPut, httpClient.method)
			assert.Contains(t, httpClient.path, "/n/testns/b/testbucket/o/prefix/file")
		})
	}
}

func TestSaveEmptyObjectSetsKMSKey(t *testing.T) {
	const kmsKeyID = "ocid1.key.oc1..test"
	client, err := configureClient(testConfig(testPrivateKey(t)))
	require.NoError(t, err)
	httpClient := &emptyPutHTTPClient{}
	client.HTTPClient = httpClient
	o := &OCI{cfg: &Config{
		Region:               "eu-frankfurt-1",
		Namespace:            "testns",
		Bucket:               "testbucket",
		ServerSideEncryption: SSE{KmsKeyID: kmsKeyID},
	}, client: client}

	err = o.Save("file", strings.NewReader(""))

	require.NoError(t, err)
	assert.Equal(t, kmsKeyID, httpClient.header.Get("opc-sse-kms-key-id"))
}

func TestSaveMultipartUploadSetsKMSKey(t *testing.T) {
	const kmsKeyID = "ocid1.key.oc1..test"
	cfg := testConfig(testPrivateKey(t))
	cfg.Retryer = &Retryer{MaxAttempts: 1}
	client, err := configureClient(cfg)
	require.NoError(t, err)
	httpClient := &failedUploadHTTPClient{}
	client.HTTPClient = httpClient
	o := &OCI{cfg: &Config{
		Region:               "eu-frankfurt-1",
		Namespace:            "testns",
		Bucket:               "testbucket",
		UploadConcurrency:    defaultUploadConcurrency,
		ServerSideEncryption: SSE{KmsKeyID: kmsKeyID},
	}, client: client}

	err = o.Save("file", strings.NewReader("data"))

	require.Error(t, err)
	assert.Positive(t, httpClient.calls)
	assert.Equal(t, kmsKeyID, httpClient.header.Get("opc-sse-kms-key-id"))
}

func TestSaveEmptyObjectSetsSSECHeaders(t *testing.T) {
	client, err := configureClient(testConfig(testPrivateKey(t)))
	require.NoError(t, err)
	httpClient := &emptyPutHTTPClient{}
	client.HTTPClient = httpClient
	o := &OCI{cfg: &Config{
		Region:               "eu-frankfurt-1",
		Namespace:            "testns",
		Bucket:               "testbucket",
		ServerSideEncryption: testSSECustomerConfig(),
	}, client: client}

	err = o.Save("file", strings.NewReader(""))

	require.NoError(t, err)
	assertSSECustomerHTTPHeaders(t, httpClient.header)
}

func TestSaveMultipartUploadSetsSSECHeaders(t *testing.T) {
	cfg := testConfig(testPrivateKey(t))
	cfg.Retryer = &Retryer{MaxAttempts: 1}
	client, err := configureClient(cfg)
	require.NoError(t, err)
	httpClient := &failedUploadHTTPClient{}
	client.HTTPClient = httpClient
	o := &OCI{cfg: &Config{
		Region:               "eu-frankfurt-1",
		Namespace:            "testns",
		Bucket:               "testbucket",
		UploadConcurrency:    defaultUploadConcurrency,
		ServerSideEncryption: testSSECustomerConfig(),
	}, client: client}

	err = o.Save("file", strings.NewReader("data"))

	require.Error(t, err)
	assert.Positive(t, httpClient.calls)
	assertSSECustomerHTTPHeaders(t, httpClient.header)
}

func testSSECustomerConfig() SSE {
	return SSE{
		SseCustomerKey: storage.MaskedString(base64.StdEncoding.EncodeToString([]byte("0123456789abcdefghijklmnopqrstuv"))),
	}
}

func assertSSECustomerHTTPHeaders(t *testing.T, header http.Header) {
	t.Helper()

	assert.Equal(t, sseCustomerAlgorithm, header.Get("opc-sse-customer-algorithm"))
	assert.Equal(t, string(testSSECustomerConfig().SseCustomerKey), header.Get("opc-sse-customer-key"))
	assert.Equal(t, testSSECustomerKeySHA256(), header.Get("opc-sse-customer-key-sha256"))
}

func assertSSECustomerRequest(t *testing.T, algorithm, key, keySHA256 *string) {
	t.Helper()

	require.NotNil(t, algorithm)
	assert.Equal(t, sseCustomerAlgorithm, *algorithm)
	require.NotNil(t, key)
	assert.Equal(t, string(testSSECustomerConfig().SseCustomerKey), *key)
	require.NotNil(t, keySHA256)
	assert.Equal(t, testSSECustomerKeySHA256(), *keySHA256)
}

func testSSECustomerKeySHA256() string {
	key, _ := base64.StdEncoding.DecodeString(string(testSSECustomerConfig().SseCustomerKey))
	sum := sha256.Sum256(key)
	return base64.StdEncoding.EncodeToString(sum[:])
}

type emptyPutHTTPClient struct {
	calls  int
	method string
	path   string
	header http.Header
}

func (c *emptyPutHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.calls++
	c.method = req.Method
	c.path = req.URL.Path
	c.header = req.Header.Clone()

	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{},
		Body:       http.NoBody,
		Request:    req,
	}, nil
}

type failedUploadHTTPClient struct {
	calls  int
	header http.Header
}

func (c *failedUploadHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.calls++
	if c.header == nil {
		c.header = req.Header.Clone()
	}

	return &http.Response{
		StatusCode: http.StatusInternalServerError,
		Status:     "500 Internal Server Error",
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(`{"code":"InternalError","message":"stop"}`)),
		Request:    req,
	}, nil
}
