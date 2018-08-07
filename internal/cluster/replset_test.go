package cluster

import (
	"io/ioutil"
	"testing"

	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/internal/testutils"
)

func loadBSONFile(file string, out interface{}) error {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	return bson.Unmarshal(bytes, out)
}

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
