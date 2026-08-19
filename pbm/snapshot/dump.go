package snapshot

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/percona/percona-backup-mongodb/pbm/archive"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

type UploadFunc func(ns, ext string, r io.Reader) error
type ProgressFunc func(ns string, bytes int64, done bool)

const progressThresholdBytes = 16 << 20

func UploadDump(
	ctx context.Context,
	dump func(archive.NewWriter) error,
	upload UploadFunc,
	compression compress.CompressionType,
	compressionLevel *int,
) (int64, error) {
	return UploadDumpWithProgress(ctx, dump, upload, compression, compressionLevel, nil)
}

func UploadDumpWithProgress(
	ctx context.Context,
	dump func(archive.NewWriter) error,
	upload UploadFunc,
	compression compress.CompressionType,
	compressionLevel *int,
	progress ProgressFunc,
) (int64, error) {
	uploadSize := int64(0)

	newWriter := func(ns string) (io.WriteCloser, error) {
		pr, pw := io.Pipe()

		compression := compression
		if ns == archive.MetaFileV2 {
			compression = compress.CompressionTypeNone
		}

		done := make(chan error)
		go func() {
			defer close(done)

			rc := &readCounter{r: pr, ns: ns, progress: progress}
			err := upload(ns, compression.Suffix(), rc)
			if err != nil {
				err = errors.Wrapf(err, "upload: %q", ns)
				pr.CloseWithError(err)
				done <- err
			}

			rc.finish()
			atomic.AddInt64(&uploadSize, rc.n)
		}()

		w, err := compress.Compress(pw, compression, compressionLevel)
		dwc := io.WriteCloser(&delegatedWriteCloser{w, funcCloser(func() error {
			err0 := w.Close()
			err1 := pw.Close()
			err2 := <-done
			return errors.Join(err0, err1, err2)
		})})
		return dwc, errors.Wrapf(err, "create compressor: %q", ns)
	}

	err := dump(newWriter)
	return atomic.LoadInt64(&uploadSize), err
}

type DownloadFunc func(filename string) (io.ReadCloser, error)

func DownloadDump(
	download DownloadFunc,
	compression compress.CompressionType,
	match archive.NSFilterFn,
	numParallelColls int,
) (io.ReadCloser, error) {
	return DownloadDumpWithProgress(download, compression, match, numParallelColls, nil)
}

func DownloadDumpWithProgress(
	download DownloadFunc,
	compression compress.CompressionType,
	match archive.NSFilterFn,
	numParallelColls int,
	progress ProgressFunc,
) (io.ReadCloser, error) {
	pr, pw := io.Pipe()

	go func() {
		newReader := func(ns string) (io.ReadCloser, error) {
			if ns != archive.MetaFile {
				ns += compression.Suffix()
			}

			r, err := download(ns)
			if err != nil {
				return nil, errors.Wrapf(err, "download: %q", ns)
			}
			if progress != nil {
				r = &readCounterCloser{ReadCloser: r, ns: ns, progress: progress}
			}

			if ns == archive.MetaFile {
				return r, nil
			}

			r, err = compress.Decompress(r, compression)
			return r, errors.Wrapf(err, "create decompressor: %q", ns)
		}

		err := archive.Compose(pw, newReader, match, numParallelColls)
		pw.CloseWithError(errors.Wrap(err, "compose"))
	}()

	return pr, nil
}

type readCounterCloser struct {
	io.ReadCloser
	ns       string
	n        int64
	pending  int64
	progress ProgressFunc
}

func (c *readCounterCloser) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.add(int64(n))
	return n, err
}

func (c *readCounterCloser) Close() error {
	err := c.ReadCloser.Close()
	c.finish()
	return err
}

func (c *readCounterCloser) add(n int64) {
	if n <= 0 {
		return
	}
	c.n += n
	c.pending += n
	if c.progress != nil && c.pending >= progressThresholdBytes {
		c.progress(c.ns, c.pending, false)
		c.pending = 0
	}
}

func (c *readCounterCloser) finish() {
	if c.progress != nil {
		c.progress(c.ns, c.pending, true)
	}
	c.pending = 0
}

type readCounter struct {
	r        io.Reader
	n        int64
	ns       string
	pending  int64
	progress ProgressFunc
}

func (c *readCounter) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.add(int64(n))
	return n, err
}

func (c *readCounter) add(n int64) {
	if n <= 0 {
		return
	}
	c.n += n
	c.pending += n
	if c.progress != nil && c.pending >= progressThresholdBytes {
		c.progress(c.ns, c.pending, false)
		c.pending = 0
	}
}

func (c *readCounter) finish() {
	if c.progress != nil {
		c.progress(c.ns, c.pending, true)
	}
	c.pending = 0
}

type funcCloser func() error

func (f funcCloser) Close() error {
	return f()
}

type delegatedWriteCloser struct {
	w io.Writer
	c io.Closer
}

func (d *delegatedWriteCloser) Write(b []byte) (int, error) {
	return d.w.Write(b)
}

func (d *delegatedWriteCloser) Close() error {
	return d.c.Close()
}
