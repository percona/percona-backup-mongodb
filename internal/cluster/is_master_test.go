package cluster

import (
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils"
)

func TestNewIsMaster(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to primary: %v", err.Error())
	}
	defer session.Close()

	i, err := NewIsMaster(session)
	if err != nil {
		t.Fatalf("Could not run NewIsMaster(): %v", err.Error())
	} else if i == nil || i.isMaster.Ok != 1 {
		t.Fatal("Got incomplete 'isMaster' output")
	}
}

func TestIsMasterIsReplset(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to primary: %v", err.Error())
	}
	defer session.Close()

	i, err := NewIsMaster(session)
	if err != nil {
		t.Fatalf("Could not run NewIsMaster(): %v", err.Error())
	} else if !i.IsReplset() {
		session.Close()
		t.Fatalf("Expected true from .IsReplset()")
	}
}

func TestIsMasterIsMongos(t *testing.T) {
	// primary (should fail)
	pSession, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to primary: %v", err.Error())
	}
	defer pSession.Close()

	pi, err := NewIsMaster(pSession)
	if err != nil {
		t.Fatalf("Could not run NewIsMaster(): %v", err.Error())
	} else if pi.IsMongos() {
		t.Fatal("Expected false from .IsMongos()")
	}
	pSession.Close()

	// mongos (should succeed)
	session, err := mgo.DialWithInfo(testutils.MongosDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to mongos: %v", err.Error())
	}
	defer session.Close()

	i, err := NewIsMaster(session)
	if err != nil {
		t.Fatalf("Could not run NewIsMaster(): %v", err.Error())
	} else if !i.IsMongos() {
		t.Fatal("Expected true from .IsMongos()")
	}
}

func TestIsMasterIsConfigServer(t *testing.T) {
	// primary (should fail)
	pSession, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to configsvr replset: %v", err.Error())
	}
	defer pSession.Close()

	pi, err := NewIsMaster(pSession)
	if err != nil {
		t.Fatalf("Could not run NewIsMaster(): %v", err.Error())
	} else if pi.IsConfigServer() {
		t.Fatal("Expected false from .IsConfigServer()")
	}
	pSession.Close()

	// configsvr (should succeed)
	session, err := mgo.DialWithInfo(testutils.ConfigsvrReplsetDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to configsvr replset: %v", err.Error())
	}
	defer session.Close()

	i, err := NewIsMaster(session)
	if err != nil {
		t.Fatalf("Could not run NewIsMaster(): %v", err.Error())
	} else if !i.IsConfigServer() {
		t.Fatal("Expected true from .IsConfigServer()")
	}
}

func TestIsMasterIsShardedCluster(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.ConfigsvrReplsetDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to configsvr replset: %v", err.Error())
	}
	defer session.Close()

	i, err := NewIsMaster(session)
	if err != nil {
		t.Fatalf("Could not run NewIsMaster(): %v", err.Error())
	} else if !i.IsShardedCluster() {
		t.Fatalf("Expected true from .IsShardedCluster()")
	}
}

func TestIsMasterIsShardServer(t *testing.T) {
	// configsvr (should fail)
	cSession, err := mgo.DialWithInfo(testutils.ConfigsvrReplsetDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to configsvr replset: %v", err.Error())
	}
	defer cSession.Close()

	ci, err := NewIsMaster(cSession)
	if err != nil {
		t.Fatalf("Could not run NewIsMaster(): %v", err.Error())
	} else if ci.IsShardServer() {
		t.Fatalf("Expected false from .IsShardServer()")
	}
	cSession.Close()

	// shardsvr (should succeed)
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to primary replset: %v", err.Error())
	}
	defer session.Close()

	i, err := NewIsMaster(session)
	if err != nil {
		t.Fatalf("Could not run NewIsMaster(): %v", err.Error())
	} else if !i.IsShardServer() {
		t.Fatalf("Expected true from .IsShardServer()")
	}
}
