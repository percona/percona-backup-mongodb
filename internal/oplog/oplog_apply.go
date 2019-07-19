package oplog

import (
	"fmt"
	"io"
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

type Common struct {
	H    int64               `bson:"h"`
	NS   string              `bson:"ns"`
	Op   string              `bson:"op"`
	T    int                 `bson:"t"`
	TS   bson.MongoTimestamp `bson:"ts"`
	UI   bson.Binary         `bson:"ui"`
	V    int                 `bson:"v"`
	Wall time.Time           `bson:"wall"`
}

type createCollectionDoc struct {
	Common `bson:",inline"`
	O      struct {
		Create      string `bson:"create"`
		Size        int    `bson:"size"`
		Max         int    `bson:"max"`
		AutoIndexID bool   `bson:"autoIndexId"`
		Capped      bool   `bson:"capped"`
		IDIndex     struct {
			Key  bson.M `bson:"key"`
			Name string `bson:"name"`
			NS   string `bson:"ns"`
			V    int    `bson:"v"`
		} `bson:"idIndex"`
	} `bson:"o"`
}

type Collation struct {
	Locale          string `bson:"locale"`
	Alternate       string `bson:"alternate"`
	CaseFirst       string `bson:"caseFirst"`
	MaxVariable     string `bson:"maxVariable"`
	Version         string `bson:"version"`
	Strength        int    `bson:"strength"`
	CaseLevel       bool   `bson:"caseLevel"`
	NumericOrdering bool   `bson:"numericOrdering"`
	Normalization   bool   `bson:"normalization"`
	Backwards       bool   `bson:"backwards"`
}

type createIndexDoc struct {
	Common `bson:",inline"`
	O      struct {
		Key              bson.D         `bson:"key"`
		V                int            `bson:"v"`
		CreateIndexes    string         `bson:"createIndexes"`
		Name             string         `bson:"name"`
		Background       bool           `bson:"background"`
		Sparse           bool           `bson:"sparse"`
		Unique           bool           `bson:"unique"`
		DropDups         bool           `bson:"dropDups"`
		PartialFilter    bson.M         `bson:"partialFilter"`
		ExpireAfter      time.Duration  `bson:"expireAfter"`
		Min              int            `bson:"min"`
		Max              int            `bson:"max"`
		Minf             float64        `bson:"minf"`
		Maxf             float64        `bson:"maxf"`
		BucketSize       float64        `bson:"bucketSize"`
		Bits             int            `bson:"bits"`
		DefaultLanguage  string         `bson:"defaultLanguage"`
		LanguageOverride string         `bson:"languageOverride"`
		Weights          map[string]int `bson:"weigths"`
		Collation        *mgo.Collation `bson:"collation"`
	} `bson:"o"`
}

type dropColDoc struct {
	Common `bson:",inline"`
	O      struct {
		Drop string `bson:"drop"`
	} `bson:"o"`
}

type dropIndexDoc struct {
	Common `bson:",inline"`
	O      struct {
		Index       string `bson:"index"`
		DropIndexes string `bson:"dropIndexes"`
	} `bson:"o"`
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
			if err == io.EOF {
				return merr
			}
			merr = multierror.Append(merr, err)
			return merr
		}

		if err := bson.Unmarshal(buf, &dest); err != nil {
			return fmt.Errorf("cannot unmarshal oplog document: %s", err)
		}

		destMap := dest.Map()

		if ts, ok := destMap["ts"].(bson.MongoTimestamp); ok {
			if oa.fcheck(ts, oa.stopAtTs) {
				return nil
			}
		} else {
			return fmt.Errorf("oplog document ts field is not bson.MongoTimestamp, it is %T", destMap["ts"])
		}

		if atomic.LoadUint32(&oa.debug) != 0 {
			fmt.Printf("%#v\n", dest)
		}

		if ns, ok := destMap["ns"]; ok {
			if nss, ok := ns.(string); ok {
				if skip(nss) {
					continue
				}
			} else {
				return fmt.Errorf("invalid type for oplog cmd ns. Want string, got %T", ns)
			}
		}

		/* TODO: Fix me
		If a collection was created by the oplog in the lines above, I still don't know how to
		preserve the collection's UUID so, remove the UUIDs from all the Insert/Update/Delete
		*/
		//if _, ok = dest["ui"]; ok {
		//	delete(dest, "ui")
		//}
		for i, val := range dest {
			if val.Name == "ui" {
				dest = append(dest[:i], dest[i+1:]...)
				break
			}
		}

		result := bson.M{}
		//TODO: Improve this. Instead of applying ops one by one (the array has one element)
		// collect several documents. What happens if there are errors? Will it fail only in
		// one operation or all operations would fail?
		if err := oa.dbSession.Run(bson.M{"applyOps": []bson.D{dest}}, result); err != nil {
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
