package restore

import (
	"io"
	"io/ioutil"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	gzip "github.com/klauspost/pgzip"
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
)

// Decompress wraps given reader by the decompressing io.ReadCloser
func Decompress(r io.Reader, c pbm.CompressionType) (io.ReadCloser, error) {
	switch c {
	case pbm.CompressionTypeGZIP, pbm.CompressionTypePGZIP:
		rr, err := gzip.NewReader(r)
		return rr, errors.Wrap(err, "gzip reader")
	case pbm.CompressionTypeLZ4:
		return ioutil.NopCloser(lz4.NewReader(r)), nil
	case pbm.CompressionTypeSNAPPY:
		return ioutil.NopCloser(snappy.NewReader(r)), nil
	case pbm.CompressionTypeS2:
		return ioutil.NopCloser(s2.NewReader(r)), nil
	case pbm.CompressionTypeZstandard:
		rr, err := zstd.NewReader(r)
		return ioutil.NopCloser(rr), errors.Wrap(err, "zstandard reader")
	default:
		return ioutil.NopCloser(r), nil
	}
}
