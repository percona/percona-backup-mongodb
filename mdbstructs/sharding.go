package mdbstructs

import "github.com/globalsign/mgo/bson"

type Shard struct {
	Id    string `bson:"_id"`
	Host  string `bson:"host"`
	State int    `bson:"state"`
}

type ListShards struct {
	Shards        []*Shard            `bson:"shards"`
	Ok            int                 `bson:"ok"`
	OperationTime bson.MongoTimestamp `bson:"operationTime"`
	ClusterTime   ClusterTime         `bson:"$clusterTime"`
}
