package oci

import (
	"bytes"
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

type emptyPutHTTPClient struct {
	calls  int
	method string
	path   string
}

func (c *emptyPutHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.calls++
	c.method = req.Method
	c.path = req.URL.Path

	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{},
		Body:       http.NoBody,
		Request:    req,
	}, nil
}
