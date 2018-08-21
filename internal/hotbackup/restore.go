package hotbackup

import (
	"github.com/globalsign/mgo"
	//"github.com/globalsign/mgo/bson"
)

type Restore struct {
	restored bool
}

func NewRestore(session *mgo.Session) (*Restore, error) {
	//err := session.Run(bson.D{{"shutdown", 1}}, nil)
	return &Restore{}, nil
}
