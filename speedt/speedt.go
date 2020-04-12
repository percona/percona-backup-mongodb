package speedt

import (
	"context"
	"io"
	"math/rand"
	"strings"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type Results struct {
	Size int
	Time time.Duration
}

const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
	TB
)

type Rand struct {
	size int
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func NewRand(size int) *Rand {
	return &Rand{
		size: size,
	}
}

func (r *Rand) Write(to io.Writer) (int, error) {
	written := 0
	b := make([]byte, 1*KB)
	for written < r.size {
		genData(b)
		n, err := to.Write(b)
		if err != nil {
			return written, err
		}
		written += n
	}
	return written, nil
}

type Collection struct {
	size int
	c    *mongo.Collection
}

func NewCollection(size int, cn *mongo.Client, namespace string) (*Collection, error) {
	ns := strings.SplitN(namespace, ".", 2)
	if len(ns) != 2 {
		return nil, errors.New("namespace should be in the format `database.collection`")
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
		if written >= c.size {
			return written, nil
		}
	}

	return written, errors.Wrap(cur.Err(), "cursor")
}

func genData(b []byte) {
	var base rune
	for i := 0; i < len(b); i++ {
		base = 'a'
		if i%5 == 0 {
			base = 'A'
		}
		b[i] = byte(rand.Int63()&25 + int64(base))
	}
}

func Compression(pbmObj *pbm.PBM, nodeCN *mongo.Client, sizeGb int, collection *string) {
	var src backup.Source
	var err error
	if collection != nil {
		src, err = NewCollection(sizeGb*GB, nodeCN, *collection)
	} else {
		src = NewRand(sizeGb * GB)
	}

	// backup.Upload(src, )
}
