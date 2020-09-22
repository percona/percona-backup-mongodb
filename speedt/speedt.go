package speedt

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Results struct {
	Size Byte
	Time time.Duration
}

func (r Results) String() string {
	return fmt.Sprintf("%v sent in %s.\nAvg upload rate = %.2fMB/s.", r.Size, r.Time.Round(time.Second), float64(r.Size/MB)/r.Time.Seconds())
}

type Byte float64

const (
	_       = iota
	KB Byte = 1 << (10 * iota)
	MB
	GB
	TB
)

func (b Byte) String() string {
	switch {
	case b >= TB:
		return fmt.Sprintf("%.2fTB", b/TB)
	case b >= GB:
		return fmt.Sprintf("%.2fGB", b/GB)
	case b >= MB:
		return fmt.Sprintf("%.2fMB", b/MB)
	case b >= KB:
		return fmt.Sprintf("%.2fKB", b/KB)
	}
	return fmt.Sprintf("%.2fB", b)
}

type Rand struct {
	size Byte
}

func NewRand(size Byte) *Rand {
	r := &Rand{
		size: size,
	}
	return r
}

func (r *Rand) WriteTo(w io.Writer) (int64, error) {
	var written int64
	for i := 0; written < int64(r.size); i++ {
		n, err := w.Write(StringToBytes(dataset[i%len(dataset)]))
		if err != nil {
			return written, err
		}
		written += int64(n)
	}
	return written, nil
}

type Collection struct {
	size Byte
	c    *mongo.Collection
}

func NewCollection(size Byte, cn *mongo.Client, namespace string) (*Collection, error) {
	ns := strings.SplitN(namespace, ".", 2)
	if len(ns) != 2 {
		return nil, errors.New("namespace should be in format `database.collection`")
	}

	return &Collection{
		size: size,
		c:    cn.Database(ns[0]).Collection(ns[1]),
	}, nil
}

func (c *Collection) WriteTo(w io.Writer) (int64, error) {
	ctx := context.Background()

	cur, err := c.c.Find(ctx, bson.M{})
	if err != nil {
		return 0, errors.Wrap(err, "create cursor")
	}
	defer cur.Close(ctx)

	var written int64
	for cur.Next(ctx) {
		n, err := w.Write([]byte(cur.Current))
		if err != nil {
			return written, errors.Wrap(err, "write")
		}
		written += int64(n)
		if written >= int64(c.size) {
			return written, nil
		}
	}

	return written, errors.Wrap(cur.Err(), "cursor")
}

func genData(b []byte) {
	for i := 0; i < len(b); i++ {
		b[i] = byte(rand.Int63()&25 + 'a')
	}
}

const fileName = "pbmSpeedTest"

func Run(nodeCN *mongo.Client, stg storage.Storage, compression pbm.CompressionType, sizeGb float64, collection string) (*Results, error) {
	var src backup.Source
	var err error
	if collection != "" {
		src, err = NewCollection(Byte(sizeGb)*GB, nodeCN, collection)
		if err != nil {
			return nil, errors.Wrap(err, "create source")
		}
	} else {
		src = NewRand(Byte(sizeGb) * GB)
	}

	r := &Results{}
	ts := time.Now()
	size, err := backup.Upload(context.Background(), src, stg, compression, fileName, -1)
	r.Size = Byte(size)
	if err != nil {
		return nil, errors.Wrap(err, "upload")
	}
	r.Time = time.Since(ts)

	return r, nil
}

// StringToBytes converts given string to the slice of bytes
// without allocations
func StringToBytes(s string) []byte {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{sh.Data, sh.Len, sh.Len}
	return *(*[]byte)(unsafe.Pointer(&bh))
}
