package cluster

import (
	"reflect"
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils"
)

func TestParseShardURI(t *testing.T) {
	rs, hosts := parseShardURI("rs/127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019")
	if rs == "" {
		t.Fatal("Got empty replset name from .parseShardURI()")
	} else if len(hosts) != 3 {
		t.Fatalf("Expected %d hosts but got %d from .parseShardURI()", 3, len(hosts))
	} else if rs != "rs" {
		t.Fatalf("Expected replset name %s but got %s from .parseShardURI()", "rs", rs)
	} else if !reflect.DeepEqual(hosts, []string{"127.0.0.1:27017", "127.0.0.1:27018", "127.0.0.1:27019"}) {
		t.Fatal("Unexpected hosts list")
	}

	// too many '/'
	rs, hosts = parseShardURI("rs///")
	if rs != "" || len(hosts) > 0 {
		t.Fatal("Expected empty results from .parseShardURI()")
	}

	// missing replset
	rs, hosts = parseShardURI("127.0.0.1:27017")
	if rs != "" || len(hosts) > 0 {
		t.Fatal("Expected empty results from .parseShardURI()")
	}
}

func TestNewShard(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo())
	if err != nil {
		t.Fatalf("Got error getting test db session: %v", err.Error())
	}
	defer session.Close()

	listShards, err := GetListShards(session)
	if err != nil {
		t.Fatalf("Got error running .GetListShards(): %v", err.Error())
	}

	shard, err := NewShard(testClusterConfig, listShards.Shards[0])
	if err != nil {
		t.Fatalf("Got error running .NewShard(): %v", err.Error())
	}
	if shard.replset.name != "rs" {
		t.Fatalf("Expected 'replset.name' to equal %v but got %v", "rs", shard.replset.name)
	} else if len(shard.replset.addrs) != 2 {
		t.Fatalf("Expected 'replset.addrs' to contain %d addresses but got %d", 3, len(shard.replset.addrs))
	}
}

func TestGetListShards(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo())
	if err != nil {
		t.Fatalf("Failed to connect to mongos: %v", err.Error())
	}
	defer session.Close()

	listShards, err := GetListShards(session)
	if err != nil {
		t.Fatalf("Failed to run 'listShards' command: %v", err.Error())
	} else if listShards.Ok != 1 {
		t.Fatal("Got non-ok response code from 'listShards' command")
	} else if len(listShards.Shards) < 1 {
		t.Fatal("Got zero shards from .GetListShards()")
	}
}

func TestGetConfigsvrShards(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.ConfigsvrReplsetDialInfo())
	if err != nil {
		t.Fatalf("Failed to connect to the configsvr replset: %v", err.Error())
	}
	defer session.Close()

	cnfsvrShards, err := GetConfigsvrShards(session)
	if err != nil {
		t.Fatalf("Failed to run .GetConfigsvrShards(): %v", err.Error())
	} else if len(cnfsvrShards) != 1 {
		t.Fatal("Got empty list of shards, should contain 1 shard")
	} else if cnfsvrShards[0].Id != testutils.MongoDBReplsetName {
		t.Fatalf("Got unexpected shard data, expected 1 shard with name %s", testutils.MongoDBReplsetName)
	}
}
