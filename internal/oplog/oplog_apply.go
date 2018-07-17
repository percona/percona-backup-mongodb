package oplog

import (
	"fmt"
	"io"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/kr/pretty"
	"github.com/percona/mongodb-backup/bsonfile"
)

type OplogApply struct {
	dbSession  *mgo.Session
	bsonReader bsonfile.BSONReader
}

func NewOplogApply(sess *mgo.Session, r bsonfile.BSONReader) (*OplogApply, error) {
	return &OplogApply{
		bsonReader: r,
		dbSession:  sess,
	}, nil
}

func (oa *OplogApply) Run() error {
	for {
		dest := bson.M{}
		if err := oa.bsonReader.UnmarshalNext(dest); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		pretty.Println(dest)
		result := bson.M{}
		err := oa.dbSession.Run(bson.M{"applyOps": []bson.M{dest}}, result)
		if err != nil {
			return fmt.Errorf("Error while applying the oplog: %s\nDocument:\n%+v", err, dest)
		}
	}
}
