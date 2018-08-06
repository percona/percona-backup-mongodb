package cluster

import (
	"testing"

	"github.com/percona/mongodb-backup/internal/testutils"
)

func TestNewReplset(t *testing.T) {
	rs, err := NewReplset(
		testutils.MongoDBReplsetName,
		[]string{
			testutils.MongoDBHost + ":" + testutils.MongoDBPrimaryPort,
		},
		testutils.MongoDBUser,
		testutils.MongoDBPassword,
	)
	if err != nil {
		t.Fatalf("Failed to create new replset struct: %v", err.Error())
	}
	if rs == nil {
		rs.Close()
		t.Fatal("Got nil replset from .NewReplset()")
	}
	rs.Close()
}

func TestGetConfig(t *testing.T) {
	rs, err := NewReplset(
		testutils.MongoDBReplsetName,
		[]string{
			testutils.MongoDBHost + ":" + testutils.MongoDBPrimaryPort,
		},
		testutils.MongoDBUser,
		testutils.MongoDBPassword,
	)
	if err != nil {
		t.Fatalf("Failed to create new replset struct: %v", err.Error())
	}
	defer rs.Close()

	config, err := rs.GetConfig()
	if err != nil {
		t.Fatalf("Failed to run .GetConfig() on Replset struct: %v", err.Error())
	} else if config.Name != testutils.MongoDBReplsetName {
		t.Fatalf("Got unexpected output from .GetConfig(), expected: %v, got %v", testutils.MongoDBReplsetName, config.Name)
	} else if len(config.Members) != 3 {
		t.Fatal("Unexpected number of replica set members in .GetConfig() result")
	}
}

func TestGetStatus(t *testing.T) {
	rs, err := NewReplset(
		testutils.MongoDBReplsetName,
		[]string{
			testutils.MongoDBHost + ":" + testutils.MongoDBPrimaryPort,
		},
		testutils.MongoDBUser,
		testutils.MongoDBPassword,
	)
	if err != nil {
		t.Fatalf("Failed to create new replset struct: %v", err.Error())
	}
	defer rs.Close()

	status, err := rs.GetStatus()
	if err != nil {
		t.Fatalf("Failed to run .GetStatus() on Replset struct: %v", err.Error())
	} else if status.Set != testutils.MongoDBReplsetName {
		t.Fatal("Got unexpected output from .GetStatus()")
	} else if len(status.Members) != 3 {
		t.Fatal("Unexpected number of replica set members in .GetStatus() result")
	}
}
