package oplog

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/hashicorp/go-multierror"
	"github.com/percona/percona-backup-mongodb/bsonfile"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type checker func(bson.MongoTimestamp, bson.MongoTimestamp) bool

type Apply struct {
	dbSession    *mgo.Session
	bsonReader   bsonfile.BSONReader
	lock         *sync.Mutex
	docsCount    int64
	stopAtTs     bson.MongoTimestamp
	fcheck       checker
	ignoreErrors bool // used for testing/debug
}

func NewOplogApply(session *mgo.Session, r bsonfile.BSONReader) (*Apply, error) {
	return &Apply{
		bsonReader: r,
		dbSession:  session.Clone(),
		lock:       &sync.Mutex{},
		stopAtTs:   -1,
		fcheck:     noCheck,
	}, nil
}

func NewOplogApplyUntil(session *mgo.Session, r bsonfile.BSONReader, stopAtTs bson.MongoTimestamp,
) (*Apply, error) {
	return &Apply{
		bsonReader: r,
		dbSession:  session.Clone(),
		lock:       &sync.Mutex{},
		stopAtTs:   stopAtTs,
		fcheck:     check,
	}, nil
}

func noCheck(ts, stopAt bson.MongoTimestamp) bool {
	return false
}

func check(ts, stopAt bson.MongoTimestamp) bool {
	return ts > stopAt
}

func (oa *Apply) Run() error {
	oa.docsCount = 0
	var merr error // multi-error
	for {
		/* dest:
		bson.M{
		    "o": bson.M{
		        "_id":  "[\xef<\x19f\xa11V\xec5>*",
		        "id":   int(49),
		        "name": "name_049",
		    },
		    "ts":   bson.MongoTimestamp(6624579654957137951),
		    "t":    int64(1),
		    "h":    int64(-8478451192930320621),
		    "v":    int(2),
		    "op":   "i",
		    "ns":   "test.test_collection",
		    "wall": time.Time{
		        wall: 0x1d905c0,
		        ext:  63678001945,
		        loc:  (*time.Location)(nil),
		    },
		}
		*/
		dest := bson.M{}
		if err := oa.bsonReader.UnmarshalNext(dest); err != nil {
			if err == io.EOF {
				return merr
			}
			merr = multierror.Append(merr, err)
			return merr
		}

		//if oa.fcheck(dest["ts"].(bson.MongoTimestamp), oa.stopAtTs) {
		//	return nil
		//}

		result := bson.M{}
		//TODO: Improve this. Instead of applying ops one by one (the array has one element)
		// collect several documents. What happens if there are errors? Will it fail only in
		// one operation or all operations would fail?
		err := oa.dbSession.Run(bson.M{"applyOps": []bson.M{dest}}, result)
		if err != nil {
			log.Errorf("cannot apply oplog: %s\n%+v\n", err, dest)
			if !oa.ignoreErrors {
				return errors.Wrap(err, "cannot apply oplog operation")
			}
			merr = multierror.Append(merr, err)
		}
		atomic.AddInt64(&oa.docsCount, 1)
	}
}

func (oa *Apply) Count() int64 {
	oa.lock.Lock()
	defer oa.lock.Unlock()
	return oa.docsCount
}
