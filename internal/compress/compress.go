package compress

import (
	"compress/gzip"
	"io"
	"runtime"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"github.com/pierrec/lz4"

	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
)

func IsValidCompressionType(s string) bool {
	switch defs.CompressionType(s) {
	case
		defs.CompressionTypeNone,
		defs.CompressionTypeGZIP,
		defs.CompressionTypePGZIP,
		defs.CompressionTypeSNAPPY,
		defs.CompressionTypeLZ4,
		defs.CompressionTypeS2,
		defs.CompressionTypeZstandard:
		return true
	}

	return false
}

// FileCompression return compression alg based on given file extension
func FileCompression(ext string) defs.CompressionType {
	switch ext {
	default:
		return defs.CompressionTypeNone
	case "gz":
		return defs.CompressionTypePGZIP
	case "lz4":
		return defs.CompressionTypeLZ4
	case "snappy":
		return defs.CompressionTypeSNAPPY
	case "s2":
		return defs.CompressionTypeS2
	case "zst":
		return defs.CompressionTypeZstandard
	}
}

// Compress makes a compressed writer from the given one
func Compress(w io.Writer, compression defs.CompressionType, level *int) (io.WriteCloser, error) {
	switch compression {
	case defs.CompressionTypeGZIP:
		if level == nil {
			level = aws.Int(gzip.DefaultCompression)
		}
		gw, err := gzip.NewWriterLevel(w, *level)
		if err != nil {
			return nil, err
		}
		return gw, nil
	case defs.CompressionTypePGZIP:
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
	case defs.CompressionTypeLZ4:
		lz4w := lz4.NewWriter(w)
		if level != nil {
			lz4w.Header.CompressionLevel = *level
		}
		return lz4w, nil
	case defs.CompressionTypeSNAPPY:
		return snappy.NewBufferedWriter(w), nil
	case defs.CompressionTypeS2:
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
	case defs.CompressionTypeZstandard:
		encLevel := zstd.SpeedDefault
		if level != nil {
			encLevel = zstd.EncoderLevelFromZstd(*level)
		}
		return zstd.NewWriter(w, zstd.WithEncoderLevel(encLevel))
	case defs.CompressionTypeNone:
		fallthrough
	default:
		return nopWriteCloser{w}, nil
	}
}

// Decompress wraps given reader by the decompressing io.ReadCloser
func Decompress(r io.Reader, c defs.CompressionType) (io.ReadCloser, error) {
	switch c {
	case defs.CompressionTypeGZIP, defs.CompressionTypePGZIP:
		rr, err := gzip.NewReader(r)
		return rr, errors.Wrap(err, "gzip reader")
	case defs.CompressionTypeLZ4:
		return io.NopCloser(lz4.NewReader(r)), nil
	case defs.CompressionTypeSNAPPY:
		return io.NopCloser(snappy.NewReader(r)), nil
	case defs.CompressionTypeS2:
		return io.NopCloser(s2.NewReader(r)), nil
	case defs.CompressionTypeZstandard:
		rr, err := zstd.NewReader(r)
		return io.NopCloser(rr), errors.Wrap(err, "zstandard reader")
	case defs.CompressionTypeNone:
		fallthrough
	default:
		return io.NopCloser(r), nil
	}
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }
