package snapshot

import (
	"io"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm/archive"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
)

type UploadDumpOptions struct {
	Compression      compress.CompressionType
	CompressionLevel *int
}

type UploadFunc func(ns, ext string, r io.Reader) error

func UploadDump(wt io.WriterTo, upload UploadFunc, opts UploadDumpOptions) error {
	pr, pw := io.Pipe()

	go func() {
		_, err := wt.WriteTo(pw)
		pw.CloseWithError(errors.WithMessage(err, "write to"))
	}()

	newWriter := func(ns string) (io.WriteCloser, error) {
		pr, pw := io.Pipe()

		go func() {
			ext := ""
			if ns != archive.MetaFile {
				ext += opts.Compression.Suffix()
			}

			err := upload(ns, ext, pr)
			if err != nil {
				pr.CloseWithError(errors.WithMessagef(err, "upload: %q", ns))
			}
		}()

		if ns == archive.MetaFile {
			return pw, nil
		}

		w, err := compress.Compress(pw, opts.Compression, opts.CompressionLevel)
		dwc := &delegatedWriteCloser{w, pw}
		return dwc, errors.WithMessagef(err, "create compressor: %q", ns)
	}

	err := archive.Decompose(pr, newWriter)
	return errors.WithMessage(err, "decompose")
}

type DownloadFunc func(filename string) (io.ReadCloser, error)

func DownloadDump(download DownloadFunc, compression compress.CompressionType, match archive.MatchFunc) (io.ReadCloser, error) {
	pr, pw := io.Pipe()

	go func() {
		newReader := func(ns string) (io.ReadCloser, error) {
			if ns != archive.MetaFile {
				ns += compression.Suffix()
			}

			r, err := download(ns)
			if err != nil {
				return nil, errors.WithMessagef(err, "download: %q", ns)
			}

			if ns == archive.MetaFile {
				return r, nil
			}

			r, err = compress.Decompress(r, compression)
			return r, errors.WithMessagef(err, "create decompressor: %q", ns)
		}

		err := archive.Compose(pw, match, newReader)
		pw.CloseWithError(errors.WithMessage(err, "compose"))
	}()

	return pr, nil
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
