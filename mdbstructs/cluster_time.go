package mdbstructs

import (
	"time"

	"github.com/globalsign/mgo/bson"
)

type ClusterTime struct {
	ClusterTime time.Time `bson:"clusterTime"`
	Signature   struct {
		Hash  bson.Binary `bson:"hash"`
		KeyID int64       `bson:"keyId"`
	} `bson:"signature"`
}
