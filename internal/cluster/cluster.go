package cluster

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/mdbstructs"
)

// GetShardingState returns a struct reflecting the output of the
// 'shardingState' server command. This command should be ran on a
// shard mongod
//
// https://docs.mongodb.com/manual/reference/command/shardingState/
//
func GetShardingState(session *mgo.Session) (*mdbstructs.ShardingState, error) {
	shardingState := mdbstructs.ShardingState{}
	err := session.Run(bson.D{{"shardingState", "1"}}, &shardingState)
	return &shardingState, err
}

// GetClusterIDShard returns the cluster ID using the result of the
// 'balancerState' server command
func GetClusterIDShard(state *mdbstructs.ShardingState) *bson.ObjectId {
	return &state.ClusterID
}

// GetClusterID returns the cluster ID using the 'config.version'
// collection. This will only succeeed on a mongos or config server,
// use .GetClusterIDShard instead on shard servers
func GetClusterID(session *mgo.Session) (*bson.ObjectId, error) {
	configVersion := struct {
		ClusterId bson.ObjectId `bson:"clusterId"`
	}{}
	err := session.DB(configDB).C("version").Find(bson.M{"_id": 1}).One(&configVersion)
	if err != nil {
		return nil, err
	}
	return &configVersion.ClusterId, nil
}
