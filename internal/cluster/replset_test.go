package cluster

import (
	"testing"

	"github.com/percona/mongodb-backup/internal/testutils"
)

func TestNewReplset(t *testing.T) {
	rs, err := NewReplset(
		testClusterConfig,
		testutils.MongoDBReplsetName,
		[]string{
			testutils.MongoDBHost + ":" + testutils.MongoDBPrimaryPort,
		},
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
		testClusterConfig,
		testutils.MongoDBReplsetName,
		[]string{
			testutils.MongoDBHost + ":" + testutils.MongoDBPrimaryPort,
		},
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

func TestGetBackupSource(t *testing.T) {
	rs, err := NewReplset(
		testClusterConfig,
		testutils.MongoDBReplsetName,
		[]string{
			testutils.MongoDBHost + ":" + testutils.MongoDBPrimaryPort,
		},
	)
	if err != nil {
		t.Fatalf("Failed to create new replset struct: %v", err.Error())
	}
	defer rs.Close()

	source, err := rs.GetBackupSource()
	if err != nil {
		t.Fatalf("Failed to run .GetBackupSource(): %v", err.Error())
	}
	if source.Host != testSecondary2Host {
		t.Fatal("Got unexpected output from .GetBackupSource()")
	}
}
