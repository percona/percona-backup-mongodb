package cluster

import (
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
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
	if !IsReplset(&mdbstructs.IsMaster{SetName: "test"}) {
		t.Fatal("Expected true from .IsReplset()")
	}
	if IsReplset(&mdbstructs.IsMaster{SetName: ""}) {
		t.Fatal("Expected false from .IsReplset()")
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
	if !IsReplset(isMaster) {
		session.Close()
		t.Fatalf("Expected true from .IsReplset()")
	}
}

func TestIsMongos(t *testing.T) {
	if !IsMongos(&mdbstructs.IsMaster{
		IsMaster: true,
		Msg:      "isdbgrid",
	}) {
		t.Fatal("Expected true from .IsMongos()")
	}
	if IsMongos(&mdbstructs.IsMaster{IsMaster: true}) {
		t.Fatal("Expected false from .IsMongos()")
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
	if !IsMongos(isMaster) {
		session.Close()
		t.Fatalf("Expected true from .IsMongos()")
	}
}

func TestIsConfigServer(t *testing.T) {
	if !IsConfigServer(&mdbstructs.IsMaster{
		ConfigSvr: 2,
		SetName:   "csReplSet",
	}) {
		t.Fatal("Expected false from .IsConfigServer()")
	}
	if IsConfigServer(&mdbstructs.IsMaster{SetName: "csReplSet"}) {
		t.Fatal("Expected true from .IsConfigServer()")
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
	if !IsConfigServer(isMaster) {
		session.Close()
		t.Fatalf("Expected true from .IsConfigServer()")
	}
}

func TestIsShardedCluster(t *testing.T) {
	if !IsShardedCluster(&mdbstructs.IsMaster{
		ConfigSvr: 2,
		SetName:   "csReplSet",
	}) {
		t.Fatal("Expected true from IsShardedCluster()")
	}
	if !IsShardedCluster(&mdbstructs.IsMaster{
		IsMaster: true,
		Msg:      "isdbgrid",
	}) {
		t.Fatal("Expected true from IsShardedCluster()")
	}
	if IsShardedCluster(&mdbstructs.IsMaster{
		IsMaster: true,
		SetName:  "test",
	}) {
		t.Fatal("Expected true false IsShardedCluster()")
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
	if !IsShardedCluster(isMaster) {
		session.Close()
		t.Fatalf("Expected true from .IsConfigServer()")
	}
}

func TestIsShardServer(t *testing.T) {
	if IsShardServer(&mdbstructs.IsMaster{
		IsMaster: true,
		SetName:  "test",
	}) {
		t.Fatal(".IsShardServer() should be false")
	}
	ts, _ := bson.NewMongoTimestamp(time.Now(), 0)
	if IsShardServer(&mdbstructs.IsMaster{
		IsMaster: true,
		Msg:      "dbgrid",
	}) {
		t.Fatal(".IsShardServer() should be false")
	}
	if !IsShardServer(&mdbstructs.IsMaster{
		IsMaster: true,
		SetName:  "test",
		ConfigServerState: &mdbstructs.ConfigServerState{
			OpTime: &mdbstructs.OpTime{
				Ts:   ts,
				Term: int64(1),
			},
		},
	}) {
		t.Fatal(".IsShardServer() should be true")
	}
}
