package oci

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

// Download holds the shared buffer state used by the concurrent downloader.
type Download struct {
	arenas   []*storage.Arena
	spanSize int
	cc       int

	stat storage.DownloadStat
}

func newDownload(cc, arenaSize, spanSize int) *Download {
	arenas := make([]*storage.Arena, 0, cc)
	for i := 0; i < cc; i++ {
		arenas = append(arenas, storage.NewArena(arenaSize, spanSize))
	}

	return &Download{
		arenas:   arenas,
		spanSize: spanSize,
		cc:       cc,
		stat:     storage.NewDownloadStat(cc, arenaSize, spanSize),
	}
}

func (o *OCI) DownloadStat() storage.DownloadStat {
	o.d.stat.Arenas = []storage.ArenaStat{}
	for _, a := range o.d.arenas {
		o.d.stat.Arenas = append(o.d.stat.Arenas, a.Stat)
	}

	return o.d.stat
}

func (o *OCI) SourceReader(name string) (io.ReadCloser, error) {
	return o.sourceReader(name, o.d.arenas, o.d.cc, o.d.spanSize)
}

func (o *OCI) newPartReader(fname string, fsize int64, chunkSize int) *storage.PartReader {
	return &storage.PartReader{
		Fname:     fname,
		Fsize:     fsize,
		ChunkSize: int64(chunkSize),
		Buf:       make([]byte, 32*1024),
		L:         o.log,
		GetChunk: func(fname string, arena *storage.Arena, cli any, start, end int64) (io.ReadCloser, error) {
			client, ok := cli.(*objectstorage.ObjectStorageClient)
			if !ok {
				return nil, errors.Errorf("expected *objectstorage.ObjectStorageClient, got %T", cli)
			}

			return o.getPartialObject(fname, arena, client, start, end-start+1)
		},
		GetSess: func() (any, error) {
			return o.client, nil
		},
	}
}

func (o *OCI) sourceReader(fname string, arenas []*storage.Arena, cc, downloadChunkSize int) (io.ReadCloser, error) {
	if cc < 1 {
		return nil, errors.Errorf("num of workers should be at least 1 (got %d)", cc)
	}
	if len(arenas) < cc {
		return nil, errors.Errorf("num of arenas (%d) less then workers (%d)", len(arenas), cc)
	}

	fstat, err := o.FileStat(fname)
	if err != nil {
		return nil, errors.Wrap(err, "get file stat")
	}

	r, w := io.Pipe()

	go func() {
		pr := o.newPartReader(fname, fstat.Size, downloadChunkSize)

		pr.Run(cc, arenas)

		exitErr := io.EOF
		defer func() {
			w.CloseWithError(exitErr)
			pr.Reset()
		}()

		cqueue := &storage.ChunksQueue{}
		heap.Init(cqueue)

		for {
			select {
			case rs := <-pr.Resultq:
				// Although chunks are requested concurrently they must be written sequentially
				// to the destination as it is not necessary a file (decompress, mongorestore etc.).
				// If it is not its turn (previous chunks weren't written yet) the chunk will be
				// added to the buffer to wait. If the buffer grows too much the scheduling of new
				// chunks will be paused for buffer to be handled.
				if rs.Meta.Start != pr.Written {
					heap.Push(cqueue, &rs)
					continue
				}

				err := pr.WriteChunk(&rs, w)
				if err != nil {
					exitErr = errors.Wrapf(err, "SourceReader: copy bytes %d-%d from response", rs.Meta.Start, rs.Meta.End)
					return
				}

				for len(*cqueue) > 0 && (*cqueue)[0].Meta.Start == pr.Written {
					r := heap.Pop(cqueue).(*storage.Chunk)
					err := pr.WriteChunk(r, w)
					if err != nil {
						exitErr = errors.Wrapf(err, "SourceReader: copy bytes %d-%d from response buffer", r.Meta.Start, r.Meta.End)
						return
					}
				}

				if pr.Written >= pr.Fsize {
					return
				}

			case err := <-pr.Errc:
				exitErr = errors.Wrapf(err, "SourceReader: download '%s/%s'", o.cfg.Bucket, fname)
				return
			}
		}
	}()

	return r, nil
}

func (o *OCI) getPartialObject(
	fname string,
	arena *storage.Arena,
	client *objectstorage.ObjectStorageClient,
	start,
	length int64,
) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	sse, err := sseHeadersFor(o.cfg.ServerSideEncryption)
	if err != nil {
		return nil, errors.Wrap(err, "server-side encryption")
	}

	res, err := client.GetObject(ctx, objectstorage.GetObjectRequest{
		NamespaceName:           common.String(o.cfg.Namespace),
		BucketName:              common.String(o.cfg.Bucket),
		ObjectName:              common.String(o.key(fname)),
		Range:                   common.String(fmt.Sprintf("bytes=%d-%d", start, start+length-1)),
		OpcSseCustomerAlgorithm: sse.customerAlgorithm,
		OpcSseCustomerKey:       sse.customerKey,
		OpcSseCustomerKeySha256: sse.customerKeySHA256,
	})
	if err != nil {
		if isRangeNotSatisfiable(err) {
			return nil, io.EOF
		}

		return nil, storage.GetObjError{Err: err}
	}
	defer res.Content.Close()

	ch := arena.GetSpan()
	_, err = io.CopyBuffer(ch, res.Content, arena.CpBuf)
	if err != nil {
		ch.Close()
		return nil, errors.Wrap(err, "copy")
	}

	return ch, nil
}

func isRangeNotSatisfiable(err error) bool {
	if se, ok := common.IsServiceError(err); ok {
		return se.GetHTTPStatusCode() == http.StatusRequestedRangeNotSatisfiable
	}
	return false
}
