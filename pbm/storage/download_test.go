package storage

import (
	stderrors "errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/percona/percona-backup-mongodb/pbm/log"
)

// TestRetryChunkDoesNotSwallowLastDownloadError covers PBM-1689: session
// re-creation must not overwrite the last download error.
func TestRetryChunkDoesNotSwallowLastDownloadError(t *testing.T) {
	wantErr := stderrors.New("download failed")

	pr := &PartReader{
		Fname:     "file",
		Fsize:     100,
		ChunkSize: 10,
		L:         log.DiscardEvent,
		GetChunk: func(string, *Arena, interface{}, int64, int64) (io.ReadCloser, error) {
			return nil, GetObjError{Err: wantErr}
		},
		GetSess: func() (interface{}, error) {
			return struct{}{}, nil
		},
	}

	arena := NewArena(1024, 1024)

	r, err := pr.retryChunk(arena, struct{}{}, 0, 9, 1)
	require.Error(t, err)
	require.Nil(t, r)
}
