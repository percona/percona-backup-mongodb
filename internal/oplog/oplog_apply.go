package oplog

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/go-version"
	"github.com/percona/percona-backup-mongodb/bsonfile"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	//	"gopkg.in/mgo.v2/bson"
)

const (
	invalidOp = iota
	createCollectionOp
	createIndexOp
	dropColOp
	dropIndexOp
)

// Oplog represents a MongoDB oplog document.
type Oplog struct {
	Timestamp bson.MongoTimestamp `bson:"ts"`
	HistoryID int64               `bson:"h"`
	Version   int                 `bson:"v"`
	Operation string              `bson:"op"`
	Namespace string              `bson:"ns"`
	Object    bson.D              `bson:"o"`
	Query     bson.D              `bson:"o2"`
	UI        *bson.Binary        `bson:"ui,omitempty"`
}

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
	needsConvert bool
}

func NewOplogApply(session *mgo.Session, r bsonfile.BSONReader) (*Apply, error) {
	session.SetMode(mgo.Strong, true)
	bi, err := session.BuildInfo()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot get build info")
	}
	v, err := version.NewVersion(bi.Version)
	if err != nil {
		return nil, errors.Wrap(err, "cannot get server version")
	}

	return &Apply{
		bsonReader:   r,
		dbSession:    session,
		lock:         &sync.Mutex{},
		stopAtTs:     -1,
		fcheck:       noCheck,
		needsConvert: needsCreateIndexWorkaround(v),
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
			return merr
		}

		if err := bson.Unmarshal(buf, &dest); err != nil {
			return fmt.Errorf("cannot unmarshal oplog document: %s", err)
		}

		destMap := dest.Map()

		if oa.fcheck(destMap["ts"].(bson.MongoTimestamp), oa.stopAtTs) {
			return nil
		}

		if ns, ok := destMap["ns"].(string); ok && skip(ns) {
			continue
		}

		if atomic.LoadUint32(&oa.debug) != 0 {
			fmt.Printf("%#v\n", dest)
		}

		result := bson.M{}
		//TODO: Improve this. Instead of applying ops one by one (the array has one element)
		// collect several documents. What happens if there are errors? Will it fail only in
		// one operation or all operations would fail?
		if destMap["o"].(bson.D)[0].Name == "createIndexes" {
			if dest, err = convertCreateIndexToIndexInsert(dest); err != nil {
				return errors.Wrap(err, "cannot convert createIndexes to insert")
			}
		}
		if err := oa.dbSession.Run(bson.M{"applyOps": []interface{}{dest}}, &result); err != nil {
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

// Server versions 3.6.0-3.6.8 and 4.0.0-4.0.2 require a 'ui' field
// in the createIndexes command.
func needsCreateIndexWorkaround(sv *version.Version) bool {
	v360, _ := version.NewVersion("3.6.0")
	v368, _ := version.NewVersion("3.6.8")
	v400, _ := version.NewVersion("4.0.0")
	v402, _ := version.NewVersion("4.0.2")

	if (sv.GreaterThanOrEqual(v360) && sv.LessThanOrEqual(v368)) ||
		(sv.GreaterThanOrEqual(v400) && sv.LessThanOrEqual(v402)) {
		return true
	}
	return false
}

func convertCreateIndexToIndexInsert(op bson.D) (bson.D, error) {
	setValue(op, "op", "i")
	op = removeValue(op, "t")

	i, err := findIndex(op, "o")
	if err != nil {
		return nil, fmt.Errorf("there is no o field")
	}
	currentOp, ok := op[i].Value.(bson.D)
	if !ok {
		return nil, fmt.Errorf("op field is not a bson.D type")
	}

	if len(currentOp) < 3 {
		return nil, fmt.Errorf("invalid number of fields in index")
	}

	nss := ""                                  // NameSpace as String = nss
	if cns := getValue(op, "ns"); cns != nil { // cns = compound namespace like: something.$cmd
		nss, _ = splitNamespace(cns.(string))
	}
	setValue(op, "ns", fmt.Sprintf("%s.system.indexes", nss))

	ns := bson.D{{Name: "ns", Value: fmt.Sprintf("%s.%s", nss, currentOp[0].Value)}}
	op[i].Value = append(currentOp[1:4], append(ns, currentOp[4:]...)...)

	return op, nil
}

func setValue(op bson.D, field string, value interface{}) {
	i, err := findIndex(op, field)
	if err != nil {
		return
	}
	op[i].Value = value
}

func getValue(op bson.D, field string) interface{} {
	i, err := findIndex(op, field)
	if err != nil {
		return nil
	}
	return op[i].Value
}

func removeValue(op bson.D, field string) bson.D {
	i, err := findIndex(op, field)
	if err != nil {
		return op
	}
	return append(op[:i], op[i+1:]...)
}

func findIndex(oplog bson.D, name string) (int, error) {
	for i, field := range oplog {
		if field.Name == name {
			return i, nil
		}
	}
	return 0, fmt.Errorf("not found")
}

func skip(ns string) bool {
	systemNS := map[string]struct{}{
		"config.system.sessions":   {},
		"config.cache.collections": {},
	}

	_, ok := systemNS[ns]

	return ok
}
func splitNamespace(namespace string) (string, string) {
	// find the first instance of "." in the namespace
	firstDotIndex := strings.Index(namespace, ".")

	// split the namespace, if applicable
	var database string
	var collection string
	if firstDotIndex != -1 {
		database = namespace[:firstDotIndex]
		collection = namespace[firstDotIndex+1:]
	} else {
		database = namespace
	}

	return database, collection
}
