package cluster

import (
	"strings"
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
)

func TestGetMongosRouters(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.ConfigsvrReplsetDialInfo(t))
	if err != nil {
		t.Fatalf("Could not connect to config server replset: %v", err.Error())
	}
	defer session.Close()

	routers, err := GetMongosRouters(session)
	if err != nil {
		t.Fatalf("Could not run .GetMongosRouters(): %v", err.Error())
	}

	if len(routers) != 1 {
		t.Fatalf("Expected %d mongos, got %d", 1, len(routers))
	}
	router := routers[0]
	if router.Up < 1 {
		t.Fatalf("Expected 'up' greater than %d, got %d", 1, router.Up)
	}
	if !strings.HasSuffix(router.Id, ":"+testutils.MongoDBMongosPort) {
		t.Fatalf("Expected router address to have suffix ':%s', got '%s'", testutils.MongoDBMongosPort, router.Id)
	}

	shardSession, err := mgo.DialWithInfo(testutils.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Fatalf("Could not connect to shard/replset: %v", err.Error())
	}
	defer shardSession.Close()

	routers, err = GetMongosRouters(shardSession)
	if err != nil {
		t.Fatalf("Could not run .GetMongosRouters(): %v", err.Error())
	}
	if len(routers) != 0 {
		t.Fatalf("Expected %d mongos, got %d", 0, len(routers))
	}
}
