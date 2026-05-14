package oci

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func TestNewPartReader(t *testing.T) {
	o := &OCI{}

	pr := o.newPartReader("test.dat", 100, 10)

	require.NotNil(t, pr)
	assert.Equal(t, "test.dat", pr.Fname)
	assert.Equal(t, int64(100), pr.Fsize)
	assert.Equal(t, int64(10), pr.ChunkSize)
	assert.Len(t, pr.Buf, 32*1024)
	require.NotNil(t, pr.GetChunk)
	require.NotNil(t, pr.GetSess)
}

func TestNewWithDownloaderConfiguresDownloadStat(t *testing.T) {
	const (
		workers    = 2
		bufferMb   = 0
		spanSizeMb = 1
	)

	expectedArenaSize, expectedSpanSize, expectedWorkers := storage.DownloadOpts(workers, bufferMb, spanSizeMb)
	stg, err := NewWithDownloader(testConfig(testPrivateKey(t)), "node", nil, workers, bufferMb, spanSizeMb)
	require.NoError(t, err)

	stat := stg.DownloadStat()
	assert.Equal(t, expectedWorkers, stat.Concurrency)
	assert.Equal(t, expectedArenaSize, stat.ArenaSize)
	assert.Equal(t, expectedSpanSize, stat.SpanSize)
	assert.Equal(t, expectedArenaSize*expectedWorkers, stat.BufSize)
}

func TestGetPartialObjectUsesRangeHeader(t *testing.T) {
	const body = "chunk"

	cfg := testConfig(testPrivateKey(t))
	client, _, err := configureClient(cfg)
	require.NoError(t, err)

	httpClient := &rangeTestHTTPClient{body: body}
	client.HTTPClient = httpClient

	o := &OCI{cfg: cfg, client: client}
	arena := storage.NewArena(1024, 1024)
	r, err := o.getPartialObject("test.dat", arena, client, 10, 11)
	require.NoError(t, err)
	defer r.Close()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, body, string(got))
	assert.Equal(t, "bytes=10-20", httpClient.rangeHeader)
}

func TestGetPartialObjectNotFoundIsGetObjError(t *testing.T) {
	cfg := testConfig(testPrivateKey(t))
	client, _, err := configureClient(cfg)
	require.NoError(t, err)

	client.HTTPClient = &statusTestHTTPClient{status: http.StatusNotFound}

	o := &OCI{cfg: cfg, client: client}
	arena := storage.NewArena(1024, 1024)
	r, err := o.getPartialObject("test.dat", arena, client, 10, 11)

	require.Nil(t, r)
	var getObjErr storage.GetObjError
	assert.True(t, errors.As(err, &getObjErr))
	assert.False(t, errors.Is(err, io.EOF))
}

func TestSourceReaderUsesRangedRequests(t *testing.T) {
	const body = "abcdefghijklmno"

	cfg := testConfig(testPrivateKey(t))
	client, _, err := configureClient(cfg)
	require.NoError(t, err)

	httpClient := &sourceReaderTestHTTPClient{body: body}
	client.HTTPClient = httpClient

	o := &OCI{
		cfg:    cfg,
		client: client,
		d:      newDownload(1, 4, 4),
	}
	r, err := o.SourceReader("test.dat")
	require.NoError(t, err)
	defer r.Close()

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Equal(t, body, string(got))
	assert.Equal(t, 1, httpClient.heads)
	assert.Equal(t, []string{"bytes=0-3", "bytes=4-7", "bytes=8-11", "bytes=12-15"}, httpClient.ranges)
}

type rangeTestHTTPClient struct {
	body        string
	rangeHeader string
}

func (c *rangeTestHTTPClient) Do(req *http.Request) (*http.Response, error) {
	c.rangeHeader = req.Header.Get("Range")

	return &http.Response{
		StatusCode: http.StatusPartialContent,
		Status:     "206 Partial Content",
		Header: http.Header{
			"Content-Length": []string{strconv.Itoa(len(c.body))},
		},
		Body:    io.NopCloser(strings.NewReader(c.body)),
		Request: req,
	}, nil
}

type statusTestHTTPClient struct {
	status int
}

func (c *statusTestHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: c.status,
		Status:     strconv.Itoa(c.status),
		Header:     make(http.Header),
		Body:       http.NoBody,
		Request:    req,
	}, nil
}

type sourceReaderTestHTTPClient struct {
	body   string
	heads  int
	ranges []string
}

func (c *sourceReaderTestHTTPClient) Do(req *http.Request) (*http.Response, error) {
	switch req.Method {
	case http.MethodHead:
		c.heads++
		return headObjectResponse(req, int64(len(c.body))), nil
	case http.MethodGet:
		return c.getObjectResponse(req), nil
	default:
		return nil, assert.AnError
	}
}

func (c *sourceReaderTestHTTPClient) getObjectResponse(req *http.Request) *http.Response {
	rangeHeader := req.Header.Get("Range")
	c.ranges = append(c.ranges, rangeHeader)

	start, end := parseRange(rangeHeader)
	data := []byte(c.body)
	if start >= len(data) {
		return &http.Response{
			StatusCode: http.StatusRequestedRangeNotSatisfiable,
			Status:     "416 Requested Range Not Satisfiable",
			Header:     make(http.Header),
			Body:       http.NoBody,
			Request:    req,
		}
	}
	if end >= len(data) {
		end = len(data) - 1
	}

	body := data[start : end+1]
	return &http.Response{
		StatusCode: http.StatusPartialContent,
		Status:     "206 Partial Content",
		Header: http.Header{
			"Content-Length": []string{strconv.Itoa(len(body))},
		},
		Body:    io.NopCloser(bytes.NewReader(body)),
		Request: req,
	}
}

func parseRange(header string) (int, int) {
	span := strings.TrimPrefix(header, "bytes=")
	parts := strings.Split(span, "-")
	start, _ := strconv.Atoi(parts[0])
	end, _ := strconv.Atoi(parts[1])

	return start, end
}
