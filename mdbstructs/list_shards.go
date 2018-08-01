package mdbstructs

import "github.com/globalsign/mgo/bson"

type ListShardsShard struct {
	Id    string `bson:"_id"`
	Host  string `bson:"host"`
	State int    `bson:"state"`
}

type ListShards struct {
	Shards        []*ListShardsShard  `bson:"shards"`
	Ok            int                 `bson:"ok"`
	OperationTime bson.MongoTimestamp `bson:"operationTime"`
	//ClusterTime ClusterTime `bson:"$clusterTime"`
}
