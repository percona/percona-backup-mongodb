package cluster

import (
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils"
	//"github.com/percona/mongodb-backup/mdbstructs"
)

func TestGetShardingState(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Failed to get primary session: %v", err.Error())
	}
	defer session.Close()

	state, err := GetShardingState(session)
	if err != nil {
		t.Fatalf("Failed to run .GetShardingState(): %v", err.Error())
	} else if state.Ok != 1 || !state.Enabled || state.ShardName != testutils.MongoDBReplsetName || state.ConfigServer == "" {
		t.Fatal("Got unexpected output from .GetShardingState()")
	}
}

func TestGetClusterID(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo())
	if err != nil {
		t.Fatalf("Failed to get mongos session: %v", err.Error())
	}
	defer session.Close()

	clusterId, err := GetClusterID(session)
	if err != nil {
		t.Fatalf("Failed to run .GetClusterID(): %v", err.Error())
	} else if clusterId == nil {
		t.Fatal(".GetClusterId() returned nil id")
	}
}

func TestGetClusterIDShard(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Failed to get primary session: %v", err.Error())
	}
	defer session.Close()

	state, err := GetShardingState(session)
	if err != nil {
		t.Fatalf("Failed to run .GetShardingState(): %v", err.Error())
	}

	shardClusterId := GetClusterIDShard(state)
	if shardClusterId == nil {
		t.Fatal("Could not get cluster ID")
	}

	// check clusterId fetched from the shard/primary is same as mongos
	mongosSession, err := mgo.DialWithInfo(testutils.MongosDialInfo())
	if err != nil {
		t.Fatalf("Failed to get mongos session: %v", err.Error())
	}
	defer mongosSession.Close()

	mongosClusterId, err := GetClusterID(mongosSession)
	if err != nil {
		t.Fatalf("Failed to run .GetClusterID(): %v", err.Error())
	}

	if mongosClusterId.Hex() != shardClusterId.Hex() {
		t.Fatal("Shard and mongos cluster IDs did not match")
	}
}
