package cluster

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/mdbstructs"
)

// GetIsMaster returns a struct containing the output of the MongoDB 'isMaster'
// server command.
//
// https://docs.mongodb.com/manual/reference/command/isMaster/
//
func GetIsMaster(session *mgo.Session) (*mdbstructs.IsMaster, error) {
	isMaster := mdbstructs.IsMaster{}
	err := session.Run(bson.D{{"isMaster", "1"}}, &isMaster)
	return &isMaster, err
}

// Use the 'SetName' field to determine if a node is a member of replication.
// If 'SetName' is defined the node is a valid member.
//
func IsReplset(isMaster *mdbstructs.IsMaster) bool {
	return isMaster.SetName != ""
}

// Use a combination of the 'isMaster', 'SetName' and 'Msg'
// field to determine a node is a mongos. A mongos will always
// have 'isMaster' equal to true, no 'SetName' defined and 'Msg'
// set to 'isdbgrid'.
//
// https://github.com/mongodb/mongo/blob/v3.6/src/mongo/s/commands/cluster_is_master_cmd.cpp#L112-L113
//
func IsMongos(isMaster *mdbstructs.IsMaster) bool {
	return isMaster.IsMaster && !IsReplset(isMaster) && isMaster.Msg == "isdbgrid"
}

// Use the undocumented 'configsvr' field to determine a node
// is a config server. This node must have replication enabled.
// For unexplained reasons the value of 'configsvr' must be an int
// equal to '2'.
//
// https://github.com/mongodb/mongo/blob/v3.6/src/mongo/db/repl/replication_info.cpp#L355-L358
//
func IsConfigServer(isMaster *mdbstructs.IsMaster) bool {
	if isMaster.ConfigSvr == 2 && IsReplset(isMaster) {
		return true
	}
	return false
}

// Use the existence of the '$configServerState' field to
// determine if a node is a mongod with the 'shardsvr'
// cluster role.
//
func IsShardServer(isMaster *mdbstructs.IsMaster) bool {
	return IsReplset(isMaster) && isMaster.ConfigServerState != nil
}

// The isMaster struct is from a Sharded Cluster if the seed host
// is a valid mongos, config server or shard server.
//
func IsShardedCluster(isMaster *mdbstructs.IsMaster) bool {
	if IsConfigServer(isMaster) || IsMongos(isMaster) || IsShardServer(isMaster) {
		return true
	}
	return false
}
