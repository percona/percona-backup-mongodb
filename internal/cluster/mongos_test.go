package cluster

import (
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
		t.Fatalf("Expected 1 mongos, got %d", len(routers))
	}

	if routers[0].Addr() != "test" {
		t.Fatalf("Expected router address %s, got %s", "test", routers[0].Addrs())
	}
}
