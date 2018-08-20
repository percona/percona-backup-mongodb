package cluster

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/mdbstructs"
)

type ShardingState struct {
	state *mdbstructs.ShardingState
}

// NewShardingState returns a struct reflecting the output of the
// 'shardingState' server command. This command should be ran on a
// shard mongod
//
// https://docs.mongodb.com/manual/reference/command/shardingState/
//
func NewShardingState(session *mgo.Session) (*ShardingState, error) {
	s := ShardingState{}
	err := session.Run(bson.D{{"shardingState", "1"}}, &s.state)
	return &s, err
}

// GetClusterIDShard returns the cluster ID using the result of the
// 'balancerState' server command
func (s *ShardingState) ClusterID() *bson.ObjectId {
	return &s.state.ClusterID
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
