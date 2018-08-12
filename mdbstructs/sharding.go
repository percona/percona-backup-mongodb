package mdbstructs

import "github.com/globalsign/mgo/bson"

// Shard reflects a document in the config server 'config.shards'
// collection (or the 'shards' array of the 'listShards' server
// command).
//
// https://docs.mongodb.com/manual/reference/config-database/#config.shards
//
type Shard struct {
	Id    string `bson:"_id"`
	Host  string `bson:"host"`
	State int    `bson:"state"`
}

// ListShards reflects the output of the MongoDB 'listShards' command.
//
// https://docs.mongodb.com/manual/reference/command/listShards/
//
type ListShards struct {
	Shards        []*Shard             `bson:"shards"`
	Ok            int                  `bson:"ok"`
	OperationTime *bson.MongoTimestamp `bson:"operationTime"`
	ClusterTime   *ClusterTime         `bson:"$clusterTime"`
}

// ShardingState reflects the output of the MongoDB 'shardingState' command.
// This command should be ran on a shard server
//
// https://docs.mongodb.com/manual/reference/command/shardingState/
//
type ShardingState struct {
	Enabled           bool                           `bson:"enabled"`
	ConfigServer      string                         `bson:"configServer,omitempty"`
	ShardName         string                         `bson:"shardName,omitempty"`
	ClusterID         bson.ObjectId                  `bson:"clusterId,omitempty"`
	Versions          map[string]bson.MongoTimestamp `bson:"versions"`
	Ok                int                            `bson:"ok"`
	OperationTime     bson.MongoTimestamp            `bson:"operationTime"`
	GleStats          *GleStats                      `bson:"$gleStats,omitempty"`
	ClusterTime       *ClusterTime                   `bson:"$clusterTime,omitempty"`
	ConfigServerState *ConfigServerState             `bson:"$configServerState,omitempty"`
}

type ConfigServerState struct {
	OpTime *OpTime `bson:"opTime"`
}

type BalancerMode string

const (
	BalancerModeFull BalancerMode = "full"
	BalancerModeOff  BalancerMode = "off"
)

type BalancerStatus struct {
	Mode              BalancerMode `bson:"mode"`
	InBalancerRound   bool         `bson:"inBalancerRound"`
	NumBalancerRounds int64        `bson:"numBalancerRounds"`
	Ok                int          `bson:"ok"`
	ClusterTime       *ClusterTime `bson:"$clusterTime,omitempty"`
	OperationTime     *OpTime      `bson:"operationTime,omitempty"`
}
