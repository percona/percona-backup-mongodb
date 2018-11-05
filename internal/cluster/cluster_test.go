package cluster

import (
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils"
)

func TestNewShardingState(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Fatalf("Failed to get primary session: %v", err.Error())
	}
	defer session.Close()

	s, err := NewShardingState(session)
	if err != nil {
		t.Fatalf("Failed to run .NewShardingState(): %v", err.Error())
	} else if s.state.Ok != 1 || !s.state.Enabled || s.state.ShardName != testutils.MongoDBShard1ReplsetName || s.state.ConfigServer == "" {
		t.Fatal("Got unexpected output from .NewShardingState()")
	}
}

func TestShardingStateClusterID(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Fatalf("Failed to get primary session: %v", err.Error())
	}
	defer session.Close()

	s, err := NewShardingState(session)
	if err != nil {
		t.Fatalf("Failed to run .NewShardingState(): %v", err.Error())
	} else if s.ClusterID() == nil {
		t.Fatal("Could not get cluster ID")
	}

	// check clusterId fetched from the shard/primary is same as mongos
	mongosSession, err := mgo.DialWithInfo(testutils.MongosDialInfo(t))
	if err != nil {
		t.Fatalf("Failed to get mongos session: %v", err.Error())
	}
	defer mongosSession.Close()

	mongosClusterId, err := GetClusterID(mongosSession)
	if err != nil {
		t.Fatalf("Failed to run .GetClusterID(): %v", err.Error())
	}

	if mongosClusterId.Hex() != s.ClusterID().Hex() {
		t.Fatal("Shard and mongos cluster IDs did not match")
	}
}

func TestGetClusterID(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo(t))
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
