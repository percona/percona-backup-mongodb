package oss

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

// WriteOnlyOptions configures a WriteOnlyFile.
type WriteOnlyOptions struct {
	// PartSize is the multipart part size. Defaults to DefaultUploadPartSize.
	PartSize int64

	// ParallelNum is the number of concurrent UploadPart workers.
	// Defaults to DefaultUploadParallel.
	ParallelNum int

	// CreateParameter passes object attributes (ContentType, StorageClass, SSE,
	// Metadata, Tagging, Acl, ...) through to the created object. Fields with no
	// multipart equivalent apply only on the small-object PutObject path.
	CreateParameter *PutObjectRequest
}

// ErrCheckpointInvalid reports that a checkpoint no longer matches server state.
// OpenWriteOnlyFile refuses to resume rather than silently restarting from zero.
var ErrCheckpointInvalid = errors.New("oss: write checkpoint no longer valid")

// WriteCheckpoint is a self-describing resume token snapshot.
type WriteCheckpoint struct {
	UploadId  string // resume token; "" before initiate
	Offset    int64  // contiguous durable prefix [0, Offset); always a PartSize multiple
	CRC64     uint64 // crc64ecma of [0, Offset)
	PartSize  int64  // bound to UploadId
	LastError error  // most recent unrecoverable background error; nil when healthy
}

type donePart struct {
	etag *string
	crc  uint64
	size int64
}

type writeChunk struct {
	partNum int32
	buf     *[]byte
	size    int
}

// writeOnlyCore holds the state shared between the producer and the background
// UploadPart workers. Workers close over the core, not the public handle, so a
// dropped handle can become unreachable and let its finalizer reap them.
type writeOnlyCore struct {
	client  UploadAPIClient
	context context.Context

	bucket       string
	key          string
	requestPayer *string

	partSize  int64
	enableCRC bool

	partPool byteSlicePool
	ch       chan writeChunk
	wg       sync.WaitGroup
	partsWg  sync.WaitGroup

	// stickyErr holds the most recent unrecoverable background error, set by
	// workers and read by producer methods; atomic so reads need no lock.
	stickyErr atomic.Value // stores saveErr

	// mu-guarded snapshot state (read by StatCheckpoint concurrently)
	mu              sync.Mutex
	uploadId        string
	doneParts       map[int32]donePart
	committedOffset int64
	committedCRC64  uint64
	nextContigPart  int32
}

// WriteOnlyFile is a buffered, background-concurrent, sequential write handle
// producing a Normal object, with checkpoint/resume support.
type WriteOnlyFile struct {
	*writeOnlyCore

	createParam *PutObjectRequest
	parallelNum int

	// producer-only state (single-threaded per io.Writer contract; Write and
	// Seek are serialized by the caller, so these need no lock)
	curBuf         *[]byte
	curFilled      int
	writeCursor    int64
	assignedParts  int32
	initiated      bool
	workersStarted bool
	chClosed       bool
	closed         bool
}

// NewWriteOnlyFile builds a handle locally with no network call.
func NewWriteOnlyFile(ctx context.Context, c UploadAPIClient, bucket, key string, optFns ...func(*WriteOnlyOptions)) (*WriteOnlyFile, error) {
	if c == nil {
		return nil, NewErrParamNull("client")
	}
	if bucket == "" {
		return nil, NewErrParamNull("bucket")
	}
	if key == "" {
		return nil, NewErrParamNull("key")
	}

	options := WriteOnlyOptions{
		PartSize:    DefaultUploadPartSize,
		ParallelNum: DefaultUploadParallel,
	}
	for _, fn := range optFns {
		fn(&options)
	}
	if options.PartSize <= 0 {
		options.PartSize = DefaultUploadPartSize
	}
	if options.ParallelNum <= 0 {
		options.ParallelNum = DefaultUploadParallel
	}

	var requestPayer *string
	if options.CreateParameter != nil {
		requestPayer = options.CreateParameter.RequestPayer
	}

	f := &WriteOnlyFile{
		writeOnlyCore: &writeOnlyCore{
			client:         c,
			context:        ctx,
			bucket:         bucket,
			key:            key,
			requestPayer:   requestPayer,
			partSize:       options.PartSize,
			doneParts:      make(map[int32]donePart),
			nextContigPart: 1,
		},
		createParam: options.CreateParameter,
		parallelNum: options.ParallelNum,
	}

	switch t := c.(type) {
	case *Client:
		f.enableCRC = (t.options.FeatureFlags & FeatureEnableCRC64CheckUpload) > 0
	case *EncryptionClient:
		f.enableCRC = (t.Unwrap().options.FeatureFlags & FeatureEnableCRC64CheckUpload) > 0
	}

	// safety net: reap workers and the buffer pool if the handle is dropped
	// without Close/AbortClose. Both clear this finalizer once they run.
	runtime.SetFinalizer(f, func(f *WriteOnlyFile) {
		f.stopWorkers()
		f.releasePool()
	})

	return f, nil
}

// OpenWriteOnlyFile resumes a write session from a checkpoint snapshot.
//
// An empty UploadId yields a fresh handle.
//
// A checkpoint that no longer matches server state (upload gone, a short
// non-trailing part, or an Offset/CRC64 mismatch) is refused with
// ErrCheckpointInvalid rather than silently restarting from zero.
func OpenWriteOnlyFile(ctx context.Context, c UploadAPIClient, bucket, key string, checkpoint WriteCheckpoint, optFns ...func(*WriteOnlyOptions)) (*WriteOnlyFile, error) {
	f, err := NewWriteOnlyFile(ctx, c, bucket, key, optFns...)
	if err != nil {
		return nil, err
	}
	if checkpoint.PartSize > 0 {
		f.partSize = checkpoint.PartSize
	}
	if checkpoint.UploadId == "" {
		return f, nil // no checkpoint to resume; behaves like a fresh handle
	}
	if checkpoint.PartSize <= 0 {
		return nil, f.wrapErr("open", fmt.Errorf("%w: resume requires a positive PartSize", ErrCheckpointInvalid))
	}

	parts, err := f.listAllParts(checkpoint.UploadId)
	if err != nil {
		var serr *ServiceError
		if errors.As(err, &serr) && serr.Code == "NoSuchUpload" {
			return nil, f.wrapErr("open", fmt.Errorf("%w: upload %q not found", ErrCheckpointInvalid, checkpoint.UploadId))
		}
		return nil, f.wrapErr("open", err)
	}

	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })

	for i, p := range parts {
		if p.Size != f.partSize && i != len(parts)-1 {
			return nil, f.wrapErr("open", fmt.Errorf("%w: part %d is short (%d < %d)", ErrCheckpointInvalid, p.PartNumber, p.Size, f.partSize))
		}
	}

	// seed contiguous full-part prefix; a trailing short part stops the walk
	var (
		expected int32 = 1
		offset   int64
		crc      uint64
	)
	for _, p := range parts {
		if p.PartNumber != expected || p.Size != f.partSize {
			break
		}
		var pcrc uint64
		if f.enableCRC && p.HashCRC64 != nil {
			pcrc, _ = strconv.ParseUint(*p.HashCRC64, 10, 64)
			crc = CRC64Combine(crc, pcrc, uint64(p.Size))
		}
		f.doneParts[expected] = donePart{etag: p.ETag, crc: pcrc, size: p.Size}
		offset += p.Size
		expected++
	}

	// cross-check the caller-supplied durable prefix against server truth.
	if checkpoint.Offset != offset {
		return nil, f.wrapErr("open", fmt.Errorf("%w: offset mismatch (checkpoint %d, server %d)", ErrCheckpointInvalid, checkpoint.Offset, offset))
	}
	if f.enableCRC && checkpoint.CRC64 != 0 && checkpoint.CRC64 != crc {
		return nil, f.wrapErr("open", fmt.Errorf("%w: crc64 mismatch (checkpoint %d, server %d)", ErrCheckpointInvalid, checkpoint.CRC64, crc))
	}

	f.uploadId = checkpoint.UploadId
	f.initiated = true
	f.assignedParts = expected - 1
	f.nextContigPart = expected
	f.committedOffset = offset
	f.committedCRC64 = crc
	return f, nil
}

func (f *WriteOnlyFile) listAllParts(uploadId string) ([]Part, error) {
	var (
		all    []Part
		marker int32
	)
	for {
		res, err := f.client.ListParts(f.context, &ListPartsRequest{
			Bucket:           Ptr(f.bucket),
			Key:              Ptr(f.key),
			UploadId:         Ptr(uploadId),
			MaxParts:         1000,
			PartNumberMarker: marker,
			RequestPayer:     f.requestPayer,
		})
		if err != nil {
			return nil, err
		}
		all = append(all, res.Parts...)
		if !res.IsTruncated {
			break
		}
		marker = res.NextPartNumberMarker
	}
	return all, nil
}

func (f *WriteOnlyFile) name() string {
	return fmt.Sprintf("oss://%s/%s", f.bucket, f.key)
}

func (f *WriteOnlyFile) wrapErr(op string, err error) error {
	if err == nil || err == io.EOF {
		return err
	}
	return &os.PathError{Op: op, Path: f.name(), Err: err}
}

func (f *WriteOnlyFile) checkClosed() error {
	if f == nil {
		return os.ErrInvalid
	}
	if f.closed {
		return os.ErrClosed
	}
	return nil
}

func (f *WriteOnlyFile) checkValid(op string) error {
	if err := f.checkClosed(); err != nil {
		return err
	}
	if err := f.stickyError(); err != nil {
		return f.wrapErr(op, err)
	}
	return nil
}

func (f *writeOnlyCore) stickyError() error {
	if v := f.stickyErr.Load(); v != nil {
		return v.(saveErr).Unwrap()
	}
	return nil
}

func (f *writeOnlyCore) setSticky(err error) {
	if err == nil {
		return
	}
	f.stickyErr.Store(saveErr{Err: err})
}

// StatCheckpoint returns a consistent snapshot of the current durable state.
func (f *WriteOnlyFile) StatCheckpoint() WriteCheckpoint {
	err := f.stickyError()
	f.mu.Lock()
	defer f.mu.Unlock()
	return WriteCheckpoint{
		UploadId:  f.uploadId,
		Offset:    f.committedOffset,
		CRC64:     f.committedCRC64,
		PartSize:  f.partSize,
		LastError: err,
	}
}

// Seek supports only Seek(0, io.SeekCurrent), returning the write cursor.
func (f *WriteOnlyFile) Seek(offset int64, whence int) (int64, error) {
	if err := f.checkClosed(); err != nil {
		return 0, err
	}
	if whence != io.SeekCurrent || offset != 0 {
		return 0, f.wrapErr("seek", errors.New("WriteOnlyFile only supports Seek(0, io.SeekCurrent) to query the write position"))
	}
	return f.writeCursor, nil
}

// Write buffers p and dispatches filled parts to background workers. It blocks
// when the buffer pool is exhausted (backpressure).
func (f *WriteOnlyFile) Write(p []byte) (int, error) {
	if err := f.checkValid("write"); err != nil {
		return 0, err
	}

	total := 0
	for len(p) > 0 {
		if f.curBuf == nil {
			buf, err := f.getBuffer()
			if err != nil {
				return total, f.wrapErr("write", err)
			}
			f.curBuf = buf
			f.curFilled = 0
		}
		n := copy((*f.curBuf)[f.curFilled:f.partSize], p)
		f.curFilled += n
		p = p[n:]
		total += n
		f.writeCursor += int64(n)

		if int64(f.curFilled) == f.partSize {
			if err := f.sealFullPart(); err != nil {
				return total, f.wrapErr("write", err)
			}
		}
	}
	return total, nil
}

// WriteFrom drains r into the file, equivalent to repeated Write.
func (f *WriteOnlyFile) WriteFrom(r io.Reader) (int64, error) {
	if err := f.checkValid("writefrom"); err != nil {
		return 0, err
	}
	var written int64
	buf := make([]byte, 32*1024)
	for {
		nr, rerr := r.Read(buf)
		if nr > 0 {
			nw, werr := f.Write(buf[:nr])
			written += int64(nw)
			if werr != nil {
				return written, werr
			}
		}
		if rerr == io.EOF {
			return written, nil
		}
		if rerr != nil {
			return written, f.wrapErr("writefrom", rerr)
		}
	}
}

// Flush uploads the current partial tail as a provisional part (a later full
// seal overwrites it) without advancing the durable offset.
func (f *WriteOnlyFile) Flush() error {
	if err := f.checkValid("flush"); err != nil {
		return err
	}

	// drain in-flight dispatched parts so their errors surface and state settles
	if f.workersStarted {
		f.partsWg.Wait()
		if se := f.stickyError(); se != nil {
			return f.wrapErr("flush", se)
		}
	}

	if f.curFilled == 0 {
		return nil
	}

	if err := f.ensureInitiated(); err != nil {
		return f.wrapErr("flush", err)
	}

	// inline UploadPart on the producer goroutine (no in-flight workers now).
	partNum := f.assignedParts + 1
	res, err := f.client.UploadPart(f.context, &UploadPartRequest{
		Bucket:       Ptr(f.bucket),
		Key:          Ptr(f.key),
		UploadId:     Ptr(f.uploadId),
		PartNumber:   partNum,
		Body:         bytes.NewReader((*f.curBuf)[:f.curFilled]),
		RequestPayer: f.requestPayer,
	})
	if err != nil {
		f.setSticky(err)
		return f.wrapErr("flush", err)
	}
	_ = res // partial part: not recorded in doneParts, offset not advanced, buffer retained
	return nil
}

func (f *WriteOnlyFile) getBuffer() (*[]byte, error) {
	if f.partPool == nil {
		f.partPool = newByteSlicePool(f.partSize)
		f.partPool.ModifyCapacity(f.parallelNum + 1)
	}
	return f.partPool.Get(f.context)
}

func (f *WriteOnlyFile) initiateRequest() *InitiateMultipartUploadRequest {
	req := &InitiateMultipartUploadRequest{}
	// Acl is applied at Complete; PutObject-only fields are dropped.
	if cp := f.createParam; cp != nil {
		req.CacheControl = cp.CacheControl
		req.ContentDisposition = cp.ContentDisposition
		req.ContentEncoding = cp.ContentEncoding
		req.ContentType = cp.ContentType
		req.Expires = cp.Expires
		req.ForbidOverwrite = cp.ForbidOverwrite
		req.ServerSideEncryption = cp.ServerSideEncryption
		req.ServerSideDataEncryption = cp.ServerSideDataEncryption
		req.SSEKMSKeyId = cp.SSEKMSKeyId
		req.ServerSideEncryptionKeyId = cp.ServerSideEncryptionKeyId
		req.StorageClass = cp.StorageClass
		req.Metadata = cp.Metadata
		req.Tagging = cp.Tagging
	}
	req.Bucket = Ptr(f.bucket)
	req.Key = Ptr(f.key)
	if f.requestPayer != nil {
		req.RequestPayer = f.requestPayer
	}
	return req
}

func (f *WriteOnlyFile) ensureInitiated() error {
	if f.initiated {
		return nil
	}
	res, err := f.client.InitiateMultipartUpload(f.context, f.initiateRequest())
	if err != nil {
		f.setSticky(err)
		return err
	}
	f.mu.Lock()
	f.uploadId = *res.UploadId
	f.initiated = true
	f.mu.Unlock()
	return nil
}

func (f *WriteOnlyFile) ensureWorkers() {
	if f.workersStarted {
		return
	}
	f.mu.Lock()
	uploadId := f.uploadId
	f.mu.Unlock()
	f.ch = make(chan writeChunk, f.parallelNum)
	for i := 0; i < f.parallelNum; i++ {
		f.wg.Add(1)
		go f.worker(uploadId)
	}
	f.workersStarted = true
}

// sealFullPart dispatches the current full buffer as the next part.
func (f *WriteOnlyFile) sealFullPart() error {
	if err := f.ensureInitiated(); err != nil {
		return err
	}
	f.ensureWorkers()
	partNum := f.assignedParts + 1
	buf := f.curBuf
	size := int(f.partSize)
	f.partsWg.Add(1)
	f.ch <- writeChunk{partNum: partNum, buf: buf, size: size}
	f.assignedParts = partNum
	f.curBuf = nil
	f.curFilled = 0
	return nil
}

func (f *writeOnlyCore) worker(uploadId string) {
	defer f.wg.Done()
	for chunk := range f.ch {
		if se := f.stickyError(); se != nil {
			f.partPool.Put(chunk.buf)
			f.partsWg.Done()
			continue
		}

		res, err := f.client.UploadPart(f.context, &UploadPartRequest{
			Bucket:       Ptr(f.bucket),
			Key:          Ptr(f.key),
			UploadId:     Ptr(uploadId),
			PartNumber:   chunk.partNum,
			Body:         bytes.NewReader((*chunk.buf)[:chunk.size]),
			RequestPayer: f.requestPayer,
		})
		if err != nil {
			f.setSticky(err)
		} else {
			var crc uint64
			if f.enableCRC && res.HashCRC64 != nil {
				crc, _ = strconv.ParseUint(*res.HashCRC64, 10, 64)
			}
			f.mu.Lock()
			f.doneParts[chunk.partNum] = donePart{etag: res.ETag, crc: crc, size: int64(chunk.size)}
			f.advanceContigLocked()
			f.mu.Unlock()
		}
		f.partPool.Put(chunk.buf)
		f.partsWg.Done()
	}
}

// advanceContigLocked advances the durable prefix over gap-free full parts.
// Callers must hold f.mu.
func (f *writeOnlyCore) advanceContigLocked() {
	for {
		dp, ok := f.doneParts[f.nextContigPart]
		if !ok || dp.size != f.partSize {
			break
		}
		if f.enableCRC {
			f.committedCRC64 = CRC64Combine(f.committedCRC64, dp.crc, uint64(dp.size))
		}
		f.committedOffset += dp.size
		f.nextContigPart++
	}
}

func (f *WriteOnlyFile) stopWorkers() {
	if f.workersStarted && !f.chClosed {
		close(f.ch)
		f.chClosed = true
		f.wg.Wait()
	}
}

// Close commits the object via CompleteMultipartUpload, or PutObject for a
// small object. On failure parts are kept for resume; it does not auto-abort.
// Calling Close on an already-closed handle is a no-op.
func (f *WriteOnlyFile) Close() error {
	if f == nil {
		return os.ErrInvalid
	}
	if f.closed {
		return nil
	}
	f.closed = true
	runtime.SetFinalizer(f, nil)

	if se := f.stickyError(); se != nil {
		f.stopWorkers()
		f.releasePool()
		return f.wrapErr("close", se)
	}

	if !f.initiated {
		err := f.putSingleObject()
		f.releasePool()
		if err != nil {
			return f.wrapErr("close", err)
		}
		return nil
	}

	// dispatch remaining tail as the last (possibly short) part
	if f.curFilled > 0 {
		f.ensureWorkers()
		partNum := f.assignedParts + 1
		buf := f.curBuf
		size := f.curFilled
		f.partsWg.Add(1)
		f.ch <- writeChunk{partNum: partNum, buf: buf, size: size}
		f.assignedParts = partNum
		f.curBuf = nil
		f.curFilled = 0
	}

	f.stopWorkers()

	if se := f.stickyError(); se != nil {
		f.releasePool()
		return f.wrapErr("close", se)
	}

	err := f.complete()
	f.releasePool()
	if err != nil {
		return f.wrapErr("close", err)
	}
	return nil
}

// AbortClose discards the upload via AbortMultipartUpload and releases the
// handle. It is mutually exclusive with Close: whichever runs first wins, and
// any later Close/AbortClose is a no-op.
func (f *WriteOnlyFile) AbortClose() error {
	if f == nil {
		return os.ErrInvalid
	}
	if f.closed {
		return nil
	}
	f.closed = true
	runtime.SetFinalizer(f, nil)

	f.stopWorkers()

	var err error
	if f.initiated {
		_, err = f.client.AbortMultipartUpload(f.context, &AbortMultipartUploadRequest{
			Bucket:       Ptr(f.bucket),
			Key:          Ptr(f.key),
			UploadId:     Ptr(f.uploadId),
			RequestPayer: f.requestPayer,
		})
	}
	f.releasePool()
	if err != nil {
		return f.wrapErr("abortclose", err)
	}
	return nil
}

func (f *WriteOnlyFile) putSingleObject() error {
	var body []byte
	if f.curBuf != nil {
		body = (*f.curBuf)[:f.curFilled]
	}
	req := &PutObjectRequest{}
	if f.createParam != nil {
		*req = *f.createParam
	}
	req.Bucket = Ptr(f.bucket)
	req.Key = Ptr(f.key)
	req.Body = bytes.NewReader(body)
	req.ContentLength = nil // body is owned here; a stale length would corrupt the upload
	req.ProgressFn = nil    // handle owns the body; the copied CreateParameter callback does not apply
	_, err := f.client.PutObject(f.context, req)
	return err
}

func (f *WriteOnlyFile) complete() error {
	f.mu.Lock()
	parts := make(UploadParts, 0, len(f.doneParts))
	for pn, dp := range f.doneParts {
		parts = append(parts, UploadPart{PartNumber: pn, ETag: dp.etag})
	}
	f.mu.Unlock()
	sort.Sort(parts)

	req := &CompleteMultipartUploadRequest{
		Bucket:                  Ptr(f.bucket),
		Key:                     Ptr(f.key),
		UploadId:                Ptr(f.uploadId),
		CompleteMultipartUpload: &CompleteMultipartUpload{Parts: parts},
		RequestPayer:            f.requestPayer,
	}
	// Acl has no Initiate equivalent, so apply it here.
	if f.createParam != nil {
		req.Acl = f.createParam.Acl
	}
	_, err := f.client.CompleteMultipartUpload(f.context, req)
	return err
}

func (f *WriteOnlyFile) releasePool() {
	if f.partPool != nil {
		f.partPool.Close()
		f.partPool = nil
	}
}
