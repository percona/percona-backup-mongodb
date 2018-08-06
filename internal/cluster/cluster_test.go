package cluster

import (
	"testing"

	"github.com/percona/mongodb-backup/mdbstructs"
)

func TestNew(t *testing.T) {
	cluster := New([]*mdbstructs.Shard{{
		Id:   "shard1",
		Host: "rs/127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019",
	}})
	if len(cluster.shards) != 1 {
		t.Fatal("Got unexpected number of shards")
	}
	shard := cluster.shards[0]
	if shard.config.Id != "shard1" {
		t.Fatal("Got unexpected shard info")
	} else if len(shard.replset.addrs) != 3 {
		t.Fatal("Got unexpected replset addresses for shard")
	}
}
