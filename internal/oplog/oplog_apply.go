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

const (
	invalidOp = iota
	createCollectionOp
	createIndexOp
	dropColOp
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
			switch getCreateType(dest) {
			case createCollectionOp:
				err := processCreateCollection(oa.dbSession, buf)
				if err != nil {
					return errors.Wrapf(err, "cannot create collection from oplog cmd %+v", dest)
				}
				continue
			case createIndexOp:
				err := processCreateIndex(oa.dbSession, buf)
				if err != nil {
					return errors.Wrapf(err, "cannot create index from oplog cmd %+v", dest)
				}
				continue
			case dropColOp:
				err := processDropColCmd(oa.dbSession, buf)
				if err != nil {
					return errors.Wrapf(err, "cannot drop collection from oplog cmd %+v", dest)
				}
				continue
			default:
				log.Errorf("unknown command in op: %+v", dest)
			}
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
	opdoc := createCollectionDoc{}
	if err := bson.Unmarshal(buf, &opdoc); err != nil {
		return errors.Wrap(err, "cannot unmarshal create collection command")
	}

	ns := strings.TrimSuffix(opdoc.NS, ".$cmd")

	if ns == "" {
		return fmt.Errorf("invalid namespace (empty)")
	}

	autoIndexID := !opdoc.O.AutoIndexID
	if _, ok := opdoc.O.IDIndex.Key["_id"]; ok {
		autoIndexID = true
	}
	info := &mgo.CollectionInfo{
		DisableIdIndex: !autoIndexID,
		ForceIdIndex:   opdoc.O.AutoIndexID,
		Capped:         opdoc.O.Capped,
		MaxBytes:       opdoc.O.Size,
		MaxDocs:        opdoc.O.Max,
	}

	if err := sess.DB(ns).C(opdoc.O.Create).Create(info); err != nil {
		return err
	}
	return nil
}

/*
   "o": bson.M{
       "key": bson.M{
           "f1": int(1),
           "f2": int(-1),
       },
       "name":          "f1_1_f2_-1",
       "background":    bool(true),
       "sparse":        bool(true),
       "createIndexes": "testcol_00",
       "v":             int(2),
       "unique":        bool(true),
   },
*/
func processCreateIndex(sess *mgo.Session, buf []byte) error {
	opdoc := createIndexDoc{}
	if err := bson.Unmarshal(buf, &opdoc); err != nil {
		return errors.Wrap(err, "cannot unmarshal create collection command")
	}

	ns := strings.TrimSuffix(opdoc.NS, ".$cmd")

	if ns == "" {
		return fmt.Errorf("invalid namespace (empty)")
	}

	index := mgo.Index{
		Key:              []string{},
		Unique:           opdoc.O.Unique,
		DropDups:         opdoc.O.DropDups,
		Background:       opdoc.O.Background,
		Sparse:           opdoc.O.Sparse,
		PartialFilter:    opdoc.O.PartialFilter,
		ExpireAfter:      opdoc.O.ExpireAfter,
		Name:             opdoc.O.Name,
		Min:              opdoc.O.Min,
		Max:              opdoc.O.Max,
		Minf:             opdoc.O.Minf,
		Maxf:             opdoc.O.Maxf,
		BucketSize:       opdoc.O.BucketSize,
		Bits:             opdoc.O.Bits,
		DefaultLanguage:  opdoc.O.DefaultLanguage,
		LanguageOverride: opdoc.O.LanguageOverride,
		Weights:          opdoc.O.Weights,
		Collation:        opdoc.O.Collation,
	}

	for _, key := range opdoc.O.Key {
		sign := ""
		switch k := key.Value.(type) {
		case int:
			if k < 0 {
				sign = "-"
			}
		case int8:
			if k < 0 {
				sign = "-"
			}
		case int16:
			if k < 0 {
				sign = "-"
			}
		case int32:
			if k < 0 {
				sign = "-"
			}
		case int64:
			if k < 0 {
				sign = "-"
			}
		case float32:
			if k < 0 {
				sign = "-"
			}
		case float64:
			if k < 0 {
				sign = "-"
			}
		}
		if key.Name != "_fts" && key.Name != "_ftsx" {
			index.Key = append(index.Key, sign+key.Name)
		}
	}
	err := sess.DB(ns).C(opdoc.O.CreateIndexes).EnsureIndex(index)
	if err != nil {
		return errors.Wrapf(err, "cannot create index %+v", index)
	}
	return nil
}

func processDropColCmd(sess *mgo.Session, buf []byte) error {
	opdoc := dropColDoc{}
	if err := bson.Unmarshal(buf, &opdoc); err != nil {
		return errors.Wrap(err, "cannot unmarshal drop collection command")
	}

	ns := strings.TrimSuffix(opdoc.NS, ".$cmd")

	if ns == "" {
		return fmt.Errorf("invalid namespace (empty)")
	}
	return sess.DB(ns).C(opdoc.O.Drop).DropCollection()
}

func getCreateType(doc bson.M) int {
	om, ok := doc["o"]
	if !ok {
		return invalidOp
	}
	o, ok := om.(bson.M)
	if !ok {
		return invalidOp
	}

	switch {
	case getString(o, "create"):
		return createCollectionOp
	case getString(o, "createIndexes"):
		return createIndexOp
	case getString(o, "drop"):
		return dropColOp
	}

	return invalidOp
}

func getString(o bson.M, key string) bool {
	if cmd, ok := o[key]; ok {
		if _, ok := cmd.(string); ok {
			return true
		}
	}
	return false
}

func (oa *Apply) Count() int64 {
	oa.lock.Lock()
	defer oa.lock.Unlock()
	return oa.docsCount
}
