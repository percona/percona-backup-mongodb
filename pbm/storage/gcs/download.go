package gcs

import (
	"container/heap"
	"context"
	"io"
	"net/http"
	"path"
	"time"

	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

type Download struct {
	gcs *GCS

	arenas   []*storage.Arena // mem buffer for downloads
	spanSize int
	cc       int // download concurrency

	stat storage.DownloadStat
}

func (g *GCS) NewDownload(cc, bufSizeMb, spanSizeMb int) *Download {
	arenaSize, spanSize, cc := storage.DownloadOpts(cc, bufSizeMb, spanSizeMb)
	g.log.Debug("download max buf %d (arena %d, span %d, concurrency %d)", arenaSize*cc, arenaSize, spanSize, cc)

	var arenas []*storage.Arena
	for i := 0; i < cc; i++ {
		arenas = append(arenas, storage.NewArena(arenaSize, spanSize))
	}

	return &Download{
		gcs:      g,
		arenas:   arenas,
		spanSize: spanSize,
		cc:       cc,
		stat:     storage.NewDownloadStat(cc, arenaSize, spanSize),
	}
}

func (d *Download) SourceReader(name string) (io.ReadCloser, error) {
	return d.gcs.sourceReader(name, d.arenas, d.cc, d.spanSize)
}

func (d *Download) Stat() storage.DownloadStat {
	d.stat.Arenas = []storage.ArenaStat{}
	for _, a := range d.arenas {
		d.stat.Arenas = append(d.stat.Arenas, a.Stat)
	}

	return d.stat
}

func (g *GCS) SourceReader(name string) (io.ReadCloser, error) {
	return g.d.SourceReader(name)
}

//// requests an object in chunks and retries if download has failed
//type partReader struct {
//	fname     string
//	fsize     int64 // a total size of object (file) to download
//	written   int64
//	chunkSize int64
//
//	getSess func() (*gcs.BucketHandle, error)
//	l       log.LogEvent
//	opts    *Config
//	buf     []byte // preallocated buf for io.Copy
//
//	taskq   chan chunkMeta
//	resultq chan chunk
//	errc    chan error
//	close   chan struct{}
//}

func (g *GCS) newPartReader(fname string, fsize int64, chunkSize int) *storage.PartReader {
	return &storage.PartReader{
		Fname:     fname,
		Fsize:     fsize,
		ChunkSize: int64(chunkSize),
		Buf:       make([]byte, 32*1024),
		L:         g.log,
		GetChunk: func(fname string, arena *storage.Arena, cli interface{}, start, end int64) (io.ReadCloser, error) {
			bucketHandle, ok := cli.(*gcs.BucketHandle)
			if !ok {
				return nil, errors.Errorf("expected *s3.Client, got %T", cli)
			}
			return g.getChunk(fname, arena, bucketHandle, start, end)
		},
		GetSess: func() (interface{}, error) {
			return g.bucketHandle, nil
		},
	}
}

func (g *GCS) sourceReader(fname string, arenas []*storage.Arena, cc, downloadChuckSize int) (io.ReadCloser, error) {
	if cc < 1 {
		return nil, errors.Errorf("num of workers shuld be at least 1 (got %d)", cc)
	}
	if len(arenas) < cc {
		return nil, errors.Errorf("num of arenas (%d) less then workers (%d)", len(arenas), cc)
	}

	fstat, err := g.FileStat(fname)
	if err != nil {
		return nil, errors.Wrap(err, "get file stat")
	}

	r, w := io.Pipe()

	go func() {
		pr := g.newPartReader(fname, fstat.Size, downloadChuckSize)

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
					exitErr = errors.Wrapf(err, "SourceReader: copy bytes %d-%d from resoponse", rs.Meta.Start, rs.Meta.End)
					return
				}

				// check if we can send something from the buffer
				for len(*cqueue) > 0 && (*cqueue)[0].Meta.Start == pr.Written {
					r := heap.Pop(cqueue).(*storage.Chunk)
					err := pr.WriteChunk(r, w)
					if err != nil {
						exitErr = errors.Wrapf(err, "SourceReader: copy bytes %d-%d from resoponse buffer", r.Meta.Start, r.Meta.End)
						return
					}
				}

				// we've read all bytes in the object
				if pr.Written >= pr.Fsize {
					return
				}

			case err := <-pr.Errc:
				exitErr = errors.Wrapf(err, "SourceReader: download '%s/%s'", g.opts.Bucket, fname)
				return
			}
		}
	}()

	return r, nil
}

func (g *GCS) getChunk(
	fname string,
	buf *storage.Arena,
	bucketHandle *gcs.BucketHandle,
	start, end int64,
) (io.ReadCloser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	length := end - start + 1
	obj := bucketHandle.Object(path.Join(g.opts.Prefix, fname))
	reader, err := obj.NewRangeReader(ctx, start, length)
	if err != nil {
		if errors.Is(err, gcs.ErrObjectNotExist) || (err != nil && isRangeNotSatisfiable(err)) {
			return nil, io.EOF
		}

		g.log.Warning("errGetObj Err: %v", err)
		return nil, storage.GetObjError{Err: err}
	}

	ch := buf.GetSpan()
	_, err = io.CopyBuffer(ch, reader, buf.CpBuf)
	if err != nil {
		ch.Close()
		return nil, errors.Wrap(err, "copy")
	}
	reader.Close()
	return ch, nil
}

func isRangeNotSatisfiable(err error) bool {
	var herr *googleapi.Error
	if errors.As(err, &herr) {
		if herr.Code == http.StatusRequestedRangeNotSatisfiable {
			return true
		}
	}
	return false
}
