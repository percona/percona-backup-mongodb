package cluster

import (
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils"
)

func TestGetConfig(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to replset: %v", err.Error())
	}
	defer session.Close()

	config, err := GetConfig(session)
	if err != nil {
		t.Fatalf("Failed to run .GetConfig() on Replset struct: %v", err.Error())
	} else if config.Name != testutils.MongoDBReplsetName {
		t.Fatalf("Got unexpected output from .GetConfig(), expected: %v, got %v", testutils.MongoDBReplsetName, config.Name)
	} else if len(config.Members) != 3 {
		t.Fatal("Unexpected number of replica set members in .GetConfig() result")
	}
}

func TestGetBackupSource(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to replset: %v", err.Error())
	}
	defer session.Close()

	config, err := GetConfig(session)
	if err != nil {
		t.Fatalf("Could not get config from replset: %v", err.Error())
	}

	status, err := GetStatus(session)
	if err != nil {
		t.Fatalf("Could not get status from replset: %v", err.Error())
	}

	source, err := GetBackupSource(config, status)
	if err != nil {
		t.Fatalf("Failed to run .GetBackupSource(): %v", err.Error())
	}
	if source.Host != testSecondary2Host {
		t.Fatal("Got unexpected output from .GetBackupSource()")
	}
}
