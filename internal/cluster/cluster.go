package cluster

import (
	"github.com/percona/mongodb-backup/mdbstructs"
)

type Cluster struct {
	shards []*Shard
}

func New(shards []*mdbstructs.Shard) *Cluster {
	c := &Cluster{
		shards: make([]*Shard, 0),
	}
	for _, shard := range shards {
		c.shards = append(c.shards, NewShard(shard))
	}
	return c
}
