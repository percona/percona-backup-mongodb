package cluster

import (
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils"
	"github.com/percona/mongodb-backup/mdbstructs"
)

func TestGetIsMaster(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to primary: %v", err.Error())
	}
	defer session.Close()

	isMaster, err := GetIsMaster(session)
	if err != nil {
		t.Fatalf("Could not run 'isMaster' command: %v", err.Error())
	}
	if isMaster == nil || isMaster.Ok != 1 {
		t.Fatal("Got incomplete 'isMaster' output")
	}
}

func TestIsReplset(t *testing.T) {
	if !isReplset(&mdbstructs.IsMaster{SetName: "test"}) {
		t.Fatal("Expected true from .isReplset()")
	}
	if isReplset(&mdbstructs.IsMaster{SetName: ""}) {
		t.Fatal("Expected false from .isReplset()")
	}

	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to primary: %v", err.Error())
	}
	defer session.Close()

	isMaster, err := GetIsMaster(session)
	if err != nil {
		session.Close()
		t.Fatalf("Could not run 'isMaster' command: %v", err.Error())
	}
	if !isReplset(isMaster) {
		session.Close()
		t.Fatalf("Expected true from .isReplset()")
	}
}

func TestIsMongos(t *testing.T) {
	if !isMongos(&mdbstructs.IsMaster{
		IsMaster: true,
		Msg:      "isdbgrid",
	}) {
		t.Fatal("Expected true from .isMongos()")
	}
	if isMongos(&mdbstructs.IsMaster{IsMaster: true}) {
		t.Fatal("Expected false from .isMongos()")
	}

	session, err := mgo.DialWithInfo(testutils.MongosDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to mongos: %v", err.Error())
	}
	defer session.Close()

	isMaster, err := GetIsMaster(session)
	if err != nil {
		session.Close()
		t.Fatalf("Could not run 'isMaster' command: %v", err.Error())
	}
	if !isMongos(isMaster) {
		session.Close()
		t.Fatalf("Expected true from .isMongos()")
	}
}

func TestIsConfigServer(t *testing.T) {
	if !isConfigServer(&mdbstructs.IsMaster{
		ConfigSvr: 2,
		SetName:   "csReplSet",
	}) {
		t.Fatal("Expected false from .isConfigServer()")
	}
	if isConfigServer(&mdbstructs.IsMaster{SetName: "csReplSet"}) {
		t.Fatal("Expected true from .isConfigServer()")
	}

	session, err := mgo.DialWithInfo(testutils.ConfigsvrReplsetDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to configsvr replset: %v", err.Error())
	}
	defer session.Close()

	isMaster, err := GetIsMaster(session)
	if err != nil {
		session.Close()
		t.Fatalf("Could not run 'isMaster' command: %v", err.Error())
	}
	if !isConfigServer(isMaster) {
		session.Close()
		t.Fatalf("Expected true from .isConfigServer()")
	}
}

func TestIsShardedCluster(t *testing.T) {
	if !isShardedCluster(&mdbstructs.IsMaster{
		ConfigSvr: 2,
		SetName:   "csReplSet",
	}) {
		t.Fatal("Expected true from isShardedCluster()")
	}
	if !isShardedCluster(&mdbstructs.IsMaster{
		IsMaster: true,
		Msg:      "isdbgrid",
	}) {
		t.Fatal("Expected true from isShardedCluster()")
	}
	if isShardedCluster(&mdbstructs.IsMaster{
		IsMaster: true,
		SetName:  "test",
	}) {
		t.Fatal("Expected true false isShardedCluster()")
	}

	session, err := mgo.DialWithInfo(testutils.ConfigsvrReplsetDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to configsvr replset: %v", err.Error())
	}
	defer session.Close()

	isMaster, err := GetIsMaster(session)
	if err != nil {
		session.Close()
		t.Fatalf("Could not run 'isMaster' command: %v", err.Error())
	}
	if !isShardedCluster(isMaster) {
		session.Close()
		t.Fatalf("Expected true from .isConfigServer()")
	}
}
