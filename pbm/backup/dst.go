package backup

import (
	"compress/gzip"
	"io"
	"runtime"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/pgzip"
	"github.com/pierrec/lz4"

	"github.com/aws/aws-sdk-go/aws"
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
func Compress(w io.Writer, compression pbm.CompressionType, level *int) (io.WriteCloser, error) {
	switch compression {
	case pbm.CompressionTypeGZIP:
		if level == nil {
			level = aws.Int(gzip.DefaultCompression)
		}
		gw, err := gzip.NewWriterLevel(w, *level)
		if err != nil {
			return nil, err
		}
		return gw, nil
	case pbm.CompressionTypePGZIP:
		if level == nil {
			level = aws.Int(pgzip.DefaultCompression)
		}
		pgw, err := pgzip.NewWriterLevel(w, *level)
		if err != nil {
			return nil, err
		}
		cc := runtime.NumCPU() / 2
		if cc == 0 {
			cc = 1
		}
		err = pgw.SetConcurrency(1<<20, cc)
		if err != nil {
			return nil, err
		}
		return pgw, nil
	case pbm.CompressionTypeLZ4:
		lz4w := lz4.NewWriter(w)
		if level != nil {
			lz4w.Header.CompressionLevel = *level
		}
		return lz4w, nil
	case pbm.CompressionTypeSNAPPY:
		return snappy.NewBufferedWriter(w), nil
	case pbm.CompressionTypeS2:
		cc := runtime.NumCPU() / 3
		if cc == 0 {
			cc = 1
		}
		writerOptions := []s2.WriterOption{s2.WriterConcurrency(cc)}
		if level != nil {
			switch *level {
			case 1:
				writerOptions = append(writerOptions, s2.WriterUncompressed())
			case 3:
				writerOptions = append(writerOptions, s2.WriterBetterCompression())
			case 4:
				writerOptions = append(writerOptions, s2.WriterBestCompression())
			}
		}
		return s2.NewWriter(w, writerOptions...), nil
	default:
		return NopCloser{w}, nil
	}
}
