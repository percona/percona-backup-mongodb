package cluster

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/mdbstructs"
)

type Cluster struct {
	seedSession *mgo.Session
}

func New(seedSession *mgo.Session) *Cluster {
	return &Cluster{seedSession: seedSession}
}

func (c *Cluster) getIsMaster() (*mdbstructs.IsMaster, error) {
	isMaster := mdbstructs.IsMaster{}
	err := c.seedSession.Run(bson.D{{"isMaster", "1"}}, &isMaster)
	return &isMaster, err
}

func (c *Cluster) isReplset(isMaster *mdbstructs.IsMaster) bool {
	return isMaster.SetName != ""
}

func (c *Cluster) isMongos(isMaster *mdbstructs.IsMaster) bool {
	// Use a combination of the 'isMaster', 'SetName' and 'Msg' field to determine a node is a mongos.
	// A mongos will always have 'isMaster' equal to true, no 'SetName' defined and 'Msg' is 'isdbgrid'.
	//
	// https://github.com/mongodb/mongo/blob/v3.6/src/mongo/s/commands/cluster_is_master_cmd.cpp#L112-L113
	return isMaster.IsMaster && !c.isReplset(isMaster) && isMaster.Msg == "isdbgrid"
}

func (c *Cluster) isConfigServer(isMaster *mdbstructs.IsMaster) bool {
	// Use the undocumented 'configsvr' field to determine a node is a config server. This mode must also have replication enabled.
	// For unexplained reasons the value of 'configsvr' must be equal to '2'.
	//
	// https://github.com/mongodb/mongo/blob/v3.6/src/mongo/db/repl/replication_info.cpp#L355-L358
	if isMaster.ConfigSvr == 2 && c.isReplset(isMaster) {
		return true
	}
	return false
}

func (c *Cluster) isShardedCluster(isMaster *mdbstructs.IsMaster) bool {
	if c.isConfigServer(isMaster) || c.isMongos(isMaster) {
		return true
	}
	return false
}

func (c *Cluster) getShards() ([]*Shard, error) {
	listShards := mdbstructs.ListShards{}
	shards := []*Shard{}
	err := c.seedSession.Run(bson.D{{"listShards", "1"}}, &listShards)
	return shards, err
}
