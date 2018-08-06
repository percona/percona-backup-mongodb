package cluster

import (
	"github.com/percona/mongodb-backup/mdbstructs"
)

type Cluster struct {
	shards map[string]*Shard
}

func New(shards []*mdbstructs.Shard) *Cluster {
	c := &Cluster{
		shards: make(map[string]*Shard),
	}
	for _, shard := range shards {
		c.shards[shard.Id] = NewShard(shard)
	}
	return c
}
