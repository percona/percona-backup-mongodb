package s3

import (
	"container/heap"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"path"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm/log"
)

const (
	downloadChuckSize = 8 << 20
	downloadRetries   = 10
)

func (s *S3) SetDownloadOpts(cc, bufSizeMb int) {
	if cc == 0 {
		cc = runtime.GOMAXPROCS(0)
	}

	s.cc = cc
	s.log.Debug("concurrency set to %d", s.cc)

	s.arena = []*arena{}
	// TODO: take into account bufSizeMb
	for i := 0; i < s.cc; i++ {
		s.arena = append(s.arena, newArena(downloadChuckSize*8, downloadChuckSize))
	}
}

func (s *S3) SourceReader(name string) (io.ReadCloser, error) {
	return s.sourceReader(name, s.arena, s.cc, downloadChuckSize)
}

type errGetObj error

type partReader struct {
	fname   string
	getSess func() (*s3.S3, error)
	l       *log.Event
	opts    *Conf
	fsize   int64
	written int64
	buf     []byte

	taskq   chan chunkMeta
	resultq chan chunk
	errc    chan error
	close   chan struct{}
}

func (s *S3) newPartReader(fname string, fsize int64, chunkSize int) *partReader {
	return &partReader{
		l:     s.log,
		buf:   make([]byte, 32*1024),
		opts:  &s.opts,
		fname: fname,
		fsize: fsize,
		getSess: func() (*s3.S3, error) {
			sess, err := s.s3session()
			if err != nil {
				return nil, err
			}
			sess.Client.Config.HTTPClient.Timeout = time.Second * 60
			return sess, nil
		},
	}
}

type chunkMeta struct {
	start int64
	end   int64
}

type chunk struct {
	r    io.ReadCloser
	meta chunkMeta
}

type chunksQueue []*chunk

func (b chunksQueue) Len() int           { return len(b) }
func (b chunksQueue) Less(i, j int) bool { return b[i].meta.start < b[j].meta.start }
func (b chunksQueue) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b *chunksQueue) Push(x any)        { *b = append(*b, x.(*chunk)) }
func (b *chunksQueue) Pop() any {
	old := *b
	n := len(old)
	x := old[n-1]
	*b = old[0 : n-1]
	return x
}

func (s *S3) sourceReader(fname string, arenas []*arena, cc, downloadChuckSize int) (io.ReadCloser, error) {
	if cc < 1 {
		return nil, errors.Errorf("num of workers shuld be at least 1 (got %d)", cc)
	}
	if len(arenas) < cc {
		return nil, errors.Errorf("num of arenas (%d) less then workers (%d)", len(arenas), cc)
	}

	fstat, err := s.FileStat(fname)
	if err != nil {
		return nil, errors.Wrap(err, "get file stat")
	}

	r, w := io.Pipe()

	go func() {
		pr := s.newPartReader(fname, fstat.Size, downloadChuckSize)

		pr.Run(cc, arenas)

		exitErr := io.EOF
		defer func() {
			w.CloseWithError(exitErr)
			pr.Reset()
		}()

		cqueue := &chunksQueue{}
		heap.Init(cqueue)

		for {
			select {
			case rs := <-pr.resultq:
				// Although chunks are requested concurrently they must be written sequentially
				// to the destination as it is not necessary a file (decompress, mongorestore etc.).
				// If it is not its turn (previous chunks weren't written yet) the chunk will be
				// added to the buffer to wait. If the buffer grows too much the scheduling of new
				// chunks will be paused for buffer to be handled.
				if rs.meta.start != pr.written {
					heap.Push(cqueue, &rs)
					continue
				}

				err := pr.writeChunk(&rs, w, downloadRetries)
				if err != nil {
					exitErr = errors.Wrapf(err, "SourceReader: copy bytes %d-%d from resoponse", rs.meta.start, rs.meta.end)
					return
				}

				// check if we can send something from the buffer
				for len(*cqueue) > 0 && []*chunk(*cqueue)[0].meta.start == pr.written {
					r := heap.Pop(cqueue).(*chunk)
					err := pr.writeChunk(r, w, downloadRetries)
					if err != nil {
						exitErr = errors.Wrapf(err, "SourceReader: copy bytes %d-%d from resoponse buffer", r.meta.start, r.meta.end)
						return
					}
				}

				// we've read all bytes in the object
				if pr.written >= pr.fsize {
					return
				}

			case err := <-pr.errc:
				exitErr = errors.Wrapf(err, "SourceReader: download '%s/%s'", s.opts.Bucket, fname)
				return
			}
		}
	}()

	return r, nil
}

func (pr *partReader) Run(concurrency int, arenas []*arena) {
	pr.taskq = make(chan chunkMeta, concurrency)
	pr.resultq = make(chan chunk)
	pr.errc = make(chan error)
	pr.close = make(chan struct{})
	go func() {
		for sent := int64(0); sent <= pr.fsize; {
			select {
			case <-pr.close:
				return
			case pr.taskq <- chunkMeta{sent, sent + downloadChuckSize - 1}:
				sent += downloadChuckSize
			}
		}
	}()

	for i := 0; i < concurrency; i++ {
		go pr.worker(arenas[i])
	}
}

func (pr *partReader) Reset() {
	close(pr.close)
}

func (pr *partReader) writeChunk(r *chunk, to io.Writer, retry int) error {
	if r == nil || r.r == nil {
		return nil
	}

	b, err := io.CopyBuffer(to, r.r, pr.buf)
	pr.written += b
	r.r.Close()

	return err
}

func (pr *partReader) worker(buf *arena) {
	sess, err := pr.getSess()
	if err != nil {
		pr.errc <- errors.Wrap(err, "create session")
		return
	}

	for {
		select {
		case ch := <-pr.taskq:
			r, err := pr.retryChunk(buf, sess, ch.start, ch.end, downloadRetries)
			if err != nil {
				pr.errc <- err
				return
			}

			pr.resultq <- chunk{r: r, meta: ch}

		case <-pr.close:
			return
		}
	}
}

func (pr *partReader) retryChunk(buf *arena, s *s3.S3, start, end int64, retries int) (r io.ReadCloser, err error) {
	for i := 0; i < retries; i++ {
		r, err = pr.tryChunk(buf, s, start, end)
		if err == nil {
			return r, nil
		}

		pr.l.Warning("retryChunk got %v, try to reconnect in %v", err, time.Second*time.Duration(i))
		time.Sleep(time.Second * time.Duration(i))
		s, err = pr.getSess()
		if err != nil {
			pr.l.Warning("recreate session err: %v", err)
			continue
		}
		pr.l.Info("session recreated, resuming download")
	}

	return nil, err
}

func (pr *partReader) tryChunk(buf *arena, s *s3.S3, start, end int64) (r io.ReadCloser, err error) {
	// just quickly retry w/o new session in case of fail.
	// more sophisticated retry on a caller side.
	const retry = 2
	for i := 0; i < retry; i++ {
		r, err = pr.getChunk(buf, s, start, end)

		if err == nil || err == io.EOF {
			return r, nil
		}
		switch err.(type) {
		case errGetObj:
			return r, err
		}

		pr.l.Warning("failed to download chunk %d-%d", start, end)
	}

	return nil, errors.Wrapf(err, "failed to download chunk %d-%d (of %d) after %d retries", start, end, pr.fsize, retry)
}

func (pr *partReader) getChunk(buf *arena, s *s3.S3, start, end int64) (io.ReadCloser, error) {
	getObjOpts := &s3.GetObjectInput{
		Bucket: aws.String(pr.opts.Bucket),
		Key:    aws.String(path.Join(pr.opts.Prefix, pr.fname)),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	}

	sse := pr.opts.ServerSideEncryption
	if sse != nil && sse.SseCustomerAlgorithm != "" {
		getObjOpts.SSECustomerAlgorithm = aws.String(sse.SseCustomerAlgorithm)
		decodedKey, err := base64.StdEncoding.DecodeString(sse.SseCustomerKey)
		getObjOpts.SSECustomerKey = aws.String(string(decodedKey[:]))
		if err != nil {
			return nil, errors.Wrap(err, "SseCustomerAlgorithm specified with invalid SseCustomerKey")
		}
		keyMD5 := md5.Sum(decodedKey[:])
		getObjOpts.SSECustomerKeyMD5 = aws.String(base64.StdEncoding.EncodeToString(keyMD5[:]))
	}

	s3obj, err := s.GetObject(getObjOpts)
	if err != nil {
		// if object size is undefined, we would read
		// until HTTP code 416 (Requested Range Not Satisfiable)
		var er awserr.RequestFailure
		if errors.As(err, &er) && er.StatusCode() == http.StatusRequestedRangeNotSatisfiable {
			return nil, io.EOF
		}
		pr.l.Warning("errGetObj Err: %v", err)
		return nil, errGetObj(err)
	}
	defer s3obj.Body.Close()

	if sse != nil {
		if sse.SseAlgorithm == s3.ServerSideEncryptionAwsKms {
			s3obj.ServerSideEncryption = aws.String(sse.SseAlgorithm)
			s3obj.SSEKMSKeyId = aws.String(sse.KmsKeyID)
		} else if sse.SseCustomerAlgorithm != "" {
			s3obj.SSECustomerAlgorithm = aws.String(sse.SseCustomerAlgorithm)
			decodedKey, _ := base64.StdEncoding.DecodeString(sse.SseCustomerKey)
			// We don't pass in the key in this case, just the MD5 hash of the key
			// for verification
			// s3obj.SSECustomerKey = aws.String(string(decodedKey[:]))
			keyMD5 := md5.Sum(decodedKey[:])
			s3obj.SSECustomerKeyMD5 = aws.String(base64.StdEncoding.EncodeToString(keyMD5[:]))
		}
	}

	ch := buf.getSpan()
	_, err = io.CopyBuffer(ch, s3obj.Body, buf.cpbuf)
	if err != nil {
		ch.Close()
		return nil, errors.Wrap(err, "copy")
	}
	return ch, nil
}

// download arena
// TODO: describe
type arena struct {
	buf       []byte
	spansize  int
	spannum   uint64
	freeindex atomic.Uint64 // buf's free spans bitmap

	cpbuf []byte
}

func newArena(size, spansize int) *arena {
	return &arena{
		buf:      make([]byte, size),
		spansize: spansize,
		spannum:  1<<(size/spansize) - 1,
		cpbuf:    make([]byte, 32*1024),
	}
}

func (b *arena) getSpan() *dspan {
	for {
		m := b.freeindex.Load()
		if m >= b.spannum {
			continue
		}
		f := firstzero(m)
		if b.freeindex.CompareAndSwap(m, m^uint64(1)<<f) {
			return &dspan{i: f * b.spansize, l: f * b.spansize, buf: b, pos: f}
		}
	}
}

func (b *arena) putSpan(c *dspan) {
	flip := uint64(1 << uint64(c.pos))
	for {
		m := b.freeindex.Load()
		if b.freeindex.CompareAndSwap(m, m&^flip) {
			return
		}
	}
}

// returns position of the first (rightmost) unset (zero) bit
func firstzero(x uint64) int {
	x = ^x
	return popcnt((x & (-x)) - 1)
}

// count the num of populated (set to 1) bits
func popcnt(x uint64) int {
	const m1 = 0x5555555555555555
	const m2 = 0x3333333333333333
	const m4 = 0x0f0f0f0f0f0f0f0f
	const h01 = 0x0101010101010101

	x -= (x >> 1) & m1
	x = (x & m2) + ((x >> 2) & m2)
	x = (x + (x >> 4)) & m4
	return int((x * h01) >> 56)
}

type dspan struct {
	i int
	l int

	pos int // position in buffer
	buf *arena
}

func (c *dspan) Write(p []byte) (n int, err error) {
	n = copy(c.buf.buf[c.l:], p)

	c.l += n
	return n, nil
}

func (c *dspan) Read(p []byte) (n int, err error) {
	n = copy(p, c.buf.buf[c.i:c.l])
	c.i += n

	if c.i == c.l {
		return n, io.EOF
	}

	return n, nil
}

func (c *dspan) Close() error {
	c.buf.putSpan(c)
	return nil
}
