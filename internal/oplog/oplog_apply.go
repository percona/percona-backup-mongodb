package oplog

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/hashicorp/go-multierror"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/percona/percona-backup-mongodb/bsonfile"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	invalidOp = iota
	createCollectionOp
	createIndexOp
	dropColOp
	dropIndexOp
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
	debug        uint32
}

func NewOplogApply(session *mgo.Session, r bsonfile.BSONReader) (*Apply, error) {
	session.SetMode(mgo.Strong, true)
	return &Apply{
		bsonReader: r,
		dbSession:  session,
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

	bsonSource := db.NewDecodedBSONSource(db.NewBufferlessBSONSource(oa.bsonReader))
	defer bsonSource.Close()
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
		dest := bson.D{}
		buf, err := oa.bsonReader.ReadNext()
		if err != nil {
			return merr
		}

		if err := bson.Unmarshal(buf, &dest); err != nil {
			return fmt.Errorf("cannot unmarshal oplog document: %s", err)
		}

		// if oa.fcheck(bson.MongoTimestamp(dest.Timestamp), oa.stopAtTs) {
		// 	return nil
		// }

		if atomic.LoadUint32(&oa.debug) != 0 {
			fmt.Printf("%#v\n", dest)
		}

		// if skip(dest.Namespace) {
		// 	continue
		// }

		/* TODO: Fix me
		If a collection was created by the oplog in the lines above, I still don't know how to
		preserve the collection's UUID so, remove the UUIDs from all the Insert/Update/Delete
		*/
		//if _, ok = dest["ui"]; ok {
		//	delete(dest, "ui")
		//}
		//for i, val := range dest {
		//	if val.Name == "ui" {
		//		dest = append(dest[:i], dest[i+1:]...)
		//		break
		//	}
		//}

		result := bson.M{}
		//TODO: Improve this. Instead of applying ops one by one (the array has one element)
		// collect several documents. What happens if there are errors? Will it fail only in
		// one operation or all operations would fail?
		if err := oa.dbSession.Run(bson.M{"applyOps": []interface{}{dest.Map()}}, result); err != nil {
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

func skip(ns string) bool {
	systemNS := map[string]struct{}{
		"config.system.sessions":   {},
		"config.cache.collections": {},
	}

	_, ok := systemNS[ns]

	return ok
}
