package oplog

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

type oplogDoc struct {
	NS   string    `bson:"ns"`
	Wall time.Time `bson:"wall"`
	O    struct {
		Create      string `bson:"create"`
		Capped      bool   `bson:"capped"`
		Size        int    `bson:"size"`
		Max         int    `bson:"max"`
		AutoIndexID bool   `bson:"autoIndexId"`
		IDIndex     struct {
			Key  bson.M `bson:"key"`
			Name string `bson:"name"`
			NS   string `bson:"ns"`
			V    int    `bson:"v"`
		} `bson:"idIndex"`
	} `bson:"o"`
	TS bson.MongoTimestamp `bson:"ts"`
	T  int                 `bson:"t"`
	H  int64               `bson:"h"`
	V  int                 `bson:"v"`
	Op string              `bson:"op"`
	UI bson.Binary         `bson:"ui"`
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
		buf, err := oa.bsonReader.ReadNext()
		if err != nil {
			if err == io.EOF {
				return merr
			}
			merr = multierror.Append(merr, err)
			return merr
		}

		if err := bson.Unmarshal(buf, dest); err != nil {
			return fmt.Errorf("cannot unmarshal oplog document: %s", err)
		}

		if oa.fcheck(dest["ts"].(bson.MongoTimestamp), oa.stopAtTs) {
			return nil
		}

		icmd, ok := dest["op"]
		if !ok {
			return fmt.Errorf("invalid oplog document. there is no command")
		}

		if cmd, ok := icmd.(string); ok && cmd == "c" {
			err := processCreateCollection(oa.dbSession, buf)
			if err != nil {
				return fmt.Errorf("cannot create collection from oplog: %s, %+v", err, dest)
			}
			oa.dbSession.Refresh()
			continue
		}

		/* TODO: Fix me
		If a collection was created by the oplog in the lines above, I still don't know how to
		preserve the collection's UUID so, remove the UUIDs from all the Insert/Update/Delete
		*/
		if _, ok = dest["ui"]; ok {
			delete(dest, "ui")
		}

		result := bson.M{}
		//TODO: Improve this. Instead of applying ops one by one (the array has one element)
		// collect several documents. What happens if there are errors? Will it fail only in
		// one operation or all operations would fail?
		if err := oa.dbSession.Run(bson.M{"applyOps": []bson.M{dest}}, result); err != nil {
			log.Errorf("cannot apply oplog: %s\n%+v\n", err, dest)
			if !oa.ignoreErrors {
				return errors.Wrap(err, "cannot apply oplog operation")
			}
			merr = multierror.Append(merr, err)
		}
		atomic.AddInt64(&oa.docsCount, 1)
	}
}

/*
bson.M{
    "op":   "c",
    "ns":   "test.$cmd",
    "wall": time.Time{
        wall: 0x4b571c0,
        ext:  63693701482,
        loc:  (*time.Location)(nil),
    },
    "o": bson.M{
        "capped":      bool(true),
        "size":        int(1000192),
        "autoIndexId": bool(false),
        "create":      "testcol_03",
    },
    "ts": bson.MongoTimestamp(6692008652934479875),
    "t":  int64(1),
    "h":  int64(4522109498565533103),
    "v":  int(2),
}
*/
func processCreateCollection(sess *mgo.Session, buf []byte) error {
	opdoc := oplogDoc{}
	if err := bson.Unmarshal(buf, &opdoc); err != nil {
		return errors.Wrap(err, "cannot unmarshal create collection command")
	}

	ns := strings.TrimSuffix(opdoc.NS, ".$cmd")

	if ns == "" {
		return fmt.Errorf("invalid namespace (empty)")
	}

	info := &mgo.CollectionInfo{
		DisableIdIndex: !opdoc.O.AutoIndexID,
		ForceIdIndex:   opdoc.O.AutoIndexID,
		Capped:         opdoc.O.Capped,
		MaxBytes:       opdoc.O.Size,
		MaxDocs:        opdoc.O.Max,
	}

	return sess.DB(ns).C(opdoc.O.Create).Create(info)
}

func (oa *Apply) Count() int64 {
	oa.lock.Lock()
	defer oa.lock.Unlock()
	return oa.docsCount
}
