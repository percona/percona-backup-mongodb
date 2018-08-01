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
	return isMaster.IsMaster && isMaster.Msg == "isdbgrid"
}

func (c *Cluster) isConfigServer(isMaster *mdbstructs.IsMaster) bool {
	if isMaster.ConfigSvr == 2 {
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
