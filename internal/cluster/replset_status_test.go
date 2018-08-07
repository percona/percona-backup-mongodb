package cluster

import (
	"testing"

	"github.com/percona/mongodb-backup/internal/testutils"
	"github.com/percona/mongodb-backup/mdbstructs"
)

const (
	testReplSetGetStatusPrimaryFile   = "testdata/replSetGetStatus-primary.bson"
	testReplSetGetStatusSecondaryFile = "testdata/replSetGetStatus-secondary.bson"
)

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

func TestGetReplsetLagDuration(t *testing.T) {
	status := mdbstructs.ReplsetStatus{}
	err := loadBSONFile(testReplSetGetStatusSecondaryFile, &status)
	if err != nil {
		t.Fatalf("Could not load test file: %v", err.Error())
	}
	t.Logf("%v\n", status)
}
