package cluster

import (
	"strings"
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils/db"
)

func TestGetMongosRouters(t *testing.T) {
	session, err := mgo.DialWithInfo(db.ConfigsvrReplsetDialInfo(t))
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
		t.Fatalf("Expected 'up' greater than 1, got %d", router.Up)
	}
	if !strings.HasSuffix(router.Addr(), ":"+testutils.MongoDBMongosPort) {
		t.Fatalf("Expected router address to have suffix ':%s', got '%s'", testutils.MongoDBMongosPort, router.Addrs())
	}
}
