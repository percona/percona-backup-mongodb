package cluster

import (
	"github.com/percona/mongodb-backup/mdbstructs"
)

type Config struct {
	Username string
	Password string
	AuthDB   string
}

type Cluster struct {
	shards map[string]*Shard
}

func New(config *Config, shards []*mdbstructs.Shard) (*Cluster, error) {
	c := &Cluster{
		shards: make(map[string]*Shard),
	}
	for _, shard := range shards {
		var err error
		c.shards[shard.Id], err = NewShard(config, shard)
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *Cluster) GetBackupSources() ([]*mdbstructs.ReplsetConfigMember, error) {
	sources := []*mdbstructs.ReplsetConfigMember{}
	for _, shard := range c.shards {
		source, err := shard.replset.GetBackupSource()
		if err != nil {
			return sources, err
		}
		sources = append(sources, source)
	}
	return sources, nil
}
