package archive

import (
	"io"
	"sync/atomic"

	"github.com/pkg/errors"
)

type UploadDumpOptions struct {
	Compression      CompressionType
	CompressionLevel *int
}

type UploadFunc func(filename string, r io.Reader) error

func UploadDump(wt io.WriterTo, upload UploadFunc, opts UploadDumpOptions) (int64, error) {
	pr, pw := io.Pipe()
	size := int64(0)

	go func() {
		_, err := wt.WriteTo(pw)
		pw.CloseWithError(errors.WithMessage(err, "write to"))
	}()

	newWriter := func(ns string) (io.WriteCloser, error) {
		pr, pw := io.Pipe()

		go func() {
			ns := ns
			if ns != MetaFile {
				ns += opts.Compression.Suffix()
			}

			rc := &readCounter{r: pr}
			err := upload(ns, rc)
			atomic.AddInt64(&size, rc.n)
			if err != nil {
				pr.CloseWithError(errors.WithMessagef(err, "upload: %q", ns))
			}
		}()

		if ns == MetaFile {
			return pw, nil
		}

		w, err := Compress(pw, opts.Compression, opts.CompressionLevel)
		dwc := &delegatedWriteCloser{w, pw}
		return dwc, errors.WithMessagef(err, "create compressor: %q", ns)
	}

	err := Decompose(pr, newWriter)
	return atomic.LoadInt64(&size), errors.WithMessage(err, "decompose")
}

type DownloadFunc func(filename string) (io.ReadCloser, error)

func DownloadDump(download DownloadFunc, compression CompressionType, match MatchFunc) (io.ReadCloser, error) {
	pr, pw := io.Pipe()

	go func() {
		newReader := func(ns string) (io.ReadCloser, error) {
			if ns != MetaFile {
				ns += compression.Suffix()
			}

			r, err := download(ns)
			if err != nil {
				return nil, errors.WithMessagef(err, "download: %q", ns)
			}

			if ns == MetaFile {
				return r, nil
			}

			r, err = Decompress(r, compression)
			return r, errors.WithMessagef(err, "create decompressor: %q", ns)
		}

		err := Compose(pw, match, newReader)
		pw.CloseWithError(errors.WithMessage(err, "compose"))
	}()

	return pr, nil
}

type readCounter struct {
	r io.Reader
	n int64
}

func (rc *readCounter) Read(b []byte) (int, error) {
	n, err := rc.r.Read(b)
	rc.n += int64(n)
	return n, err
}

type delegatedWriteCloser struct {
	w io.WriteCloser
	c io.Closer
}

func (d *delegatedWriteCloser) Write(b []byte) (int, error) {
	return d.w.Write(b)
}

func (d *delegatedWriteCloser) Close() error {
	we := d.w.Close()
	ce := d.c.Close()

	switch {
	case we != nil && ce != nil:
		return errors.Errorf("writer: %s; closer: %s", we.Error(), ce.Error())
	case we != nil:
		return errors.Errorf("writer: %s", we.Error())
	case ce != nil:
		return errors.Errorf("closer: %s", ce.Error())
	}

	return nil
}
