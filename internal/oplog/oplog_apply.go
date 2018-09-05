package oplog

import (
	"io"
	"sync"
	"sync/atomic"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/bsonfile"
	"github.com/prometheus/common/log"
)

type OplogApply struct {
	dbSession  *mgo.Session
	bsonReader bsonfile.BSONReader
	lock       *sync.Mutex
	docsCount  int64
}

func NewOplogApply(session *mgo.Session, r bsonfile.BSONReader) (*OplogApply, error) {
	return &OplogApply{
		bsonReader: r,
		dbSession:  session.Clone(),
		lock:       &sync.Mutex{},
	}, nil
}

func (oa *OplogApply) Run() error {
	oa.docsCount = 0
	for {
		dest := bson.M{}
		if err := oa.bsonReader.UnmarshalNext(dest); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		result := bson.M{}
		err := oa.dbSession.Run(bson.M{"applyOps": []bson.M{dest}}, result)
		if err != nil {
			log.Errorf("Cannot apply oplog: %s\n%+v\n", err, dest)
			return err
		}
		atomic.AddInt64(&oa.docsCount, 1)
	}
}

func (oa *OplogApply) Count() int64 {
	oa.lock.Lock()
	defer oa.lock.Unlock()
	return oa.docsCount
}
