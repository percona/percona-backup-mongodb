package backup

import (
	"compress/gzip"
	"io"
	"runtime"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/pgzip"
	"github.com/pierrec/lz4"

	"github.com/percona/percona-backup-mongodb/pbm"
)

// NopCloser wraps an io.Witer as io.WriteCloser
// with noop Close
type NopCloser struct {
	io.Writer
}

// Close to satisfy io.WriteCloser interface
func (NopCloser) Close() error { return nil }

// Compress makes a compressed writer from the given one
func Compress(w io.Writer, compression pbm.CompressionType) io.WriteCloser {
	switch compression {
	case pbm.CompressionTypeGZIP:
		return gzip.NewWriter(w)
	case pbm.CompressionTypePGZIP:
		pgw := pgzip.NewWriter(w)
		cc := runtime.NumCPU() / 2
		if cc == 0 {
			cc = 1
		}
		pgw.SetConcurrency(1<<20, cc)
		return pgw
	case pbm.CompressionTypeLZ4:
		return lz4.NewWriter(w)
	case pbm.CompressionTypeSNAPPY:
		return snappy.NewBufferedWriter(w)
	case pbm.CompressionTypeS2:
		cc := runtime.NumCPU() / 3
		if cc == 0 {
			cc = 1
		}
		return s2.NewWriter(w, s2.WriterConcurrency(cc))
	default:
		return NopCloser{w}
	}
}
