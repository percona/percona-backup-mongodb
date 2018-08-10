package cluster

import (
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils"
)

const (
	testListShardsFile = "testdata/listShards.bson"
)

func TestNew(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo())
	if err != nil {
		t.Fatalf("Got error getting test db session: %v", err.Error())
	}
	defer session.Close()

	listShards, err := GetListShards(session)
	if err != nil {
		t.Fatalf("Got error running .GetListShards(): %v", err.Error())
	}

	cluster, err := New(testClusterConfig, listShards.Shards)
	if err != nil {
		t.Fatalf("Got error running .New(): %v", err.Error())
	}
	if len(cluster.shards) != 1 {
		t.Fatal("Got unexpected number of shards")
	}
	shard := cluster.shards[testutils.MongoDBReplsetName]
	if len(shard.replset.addrs) != 2 {
		t.Fatal("Got unexpected replset addresses for shard")
	}
}

func TestGetBackupSourcesMongos(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo())
	if err != nil {
		t.Fatalf("Got error getting test db session: %v", err.Error())
	}
	defer session.Close()

	listShards, err := GetListShards(session)
	if err != nil {
		t.Fatalf("Got error running .GetListShards(): %v", err.Error())
	}

	cluster, err := New(testClusterConfig, listShards.Shards)
	if err != nil {
		t.Fatalf("Got error running .GetBackupSources(): %v", err.Error())
	}

	sources, err := cluster.GetBackupSources()
	if err != nil {
		t.Fatalf("Got error running .GetBackupSources(): %v", err.Error())
	}
	if len(sources) != 1 {
		t.Fatal(".GetBackupSources() did not return 1 backup source")
	}
	if sources[0].Host != testSecondary2Host {
		t.Fatalf(".GetBackupSources() did not return the winner: %v", testSecondary2Host)
	}
}

func TestGetBackupSourcesConfigsvr(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.ConfigsvrReplsetDialInfo())
	if err != nil {
		t.Fatalf("Got error getting test db session: %v", err.Error())
	}
	defer session.Close()

	shards, err := GetConfigsvrShards(session)
	if err != nil {
		t.Fatalf("Got error running .GetConfigsvrShards(): %v", err.Error())
	}

	cluster, err := New(testClusterConfig, shards)
	if err != nil {
		t.Fatalf("Got error running .GetBackupSources(): %v", err.Error())
	}

	sources, err := cluster.GetBackupSources()
	if err != nil {
		t.Fatalf("Got error running .GetBackupSources(): %v", err.Error())
	}
	if len(sources) != 1 {
		t.Fatal(".GetBackupSources() did not return 1 backup source")
	}
	if sources[0].Host != testSecondary2Host {
		t.Fatalf(".GetBackupSources() did not return the winner: %v", testSecondary2Host)
	}
}
