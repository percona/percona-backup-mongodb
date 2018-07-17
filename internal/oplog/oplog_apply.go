package oplog

import (
	"fmt"
	"io"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
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
		result := bson.M{}
		err := oa.dbSession.Run(bson.M{"applyOps": []bson.M{dest}}, result)
		if err != nil {
			fmt.Println("----------------------------------------------------------------------------------------------------")
			fmt.Printf("Error: %s\nDocument:\n%+v\n", err, dest)
		}
	}
}
