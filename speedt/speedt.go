package speedt

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
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
	data [1024][]byte
}

func NewRand(size Byte) *Rand {
	r := &Rand{
		size: size,
	}
	r.pregen()
	return r
}

func (r *Rand) pregen() {
	for i := 0; i < len(r.data); i++ {
		b := make([]byte, 96)
		genData(b)
		r.data[i] = b
	}
}

func (r *Rand) Write(to io.Writer) (int, error) {
	written := 0
	for i := 0; written < int(r.size); i++ {
		n, err := to.Write(r.data[i%len(r.data)])
		if err != nil {
			return written, err
		}
		written += n
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

func (c *Collection) Write(to io.Writer) (int, error) {
	ctx := context.Background()

	cur, err := c.c.Find(ctx, bson.M{})
	if err != nil {
		return 0, errors.Wrap(err, "create cursor")
	}
	defer cur.Close(ctx)

	written := 0
	for cur.Next(ctx) {
		n, err := to.Write([]byte(cur.Current))
		if err != nil {
			return written, errors.Wrap(err, "write")
		}
		written += n
		if written >= int(c.size) {
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

func Run(nodeCN *mongo.Client, stg pbm.Storage, compression pbm.CompressionType, sizeGb float64, collection string) (*Results, error) {
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
	size, err := backup.Upload(src, stg, compression, fileName)
	r.Size = Byte(size)
	if err != nil {
		return nil, errors.Wrap(err, "upload")
	}
	r.Time = time.Since(ts)

	return r, nil
}
