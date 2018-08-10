package cluster

import (
	"io/ioutil"
	"testing"

	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/internal/testutils"
	"github.com/percona/mongodb-backup/mdbstructs"
)

const (
	testReplSetConfigFile          = "testdata/replSetGetConfig.bson"
	testReplSetStatusPrimaryFile   = "testdata/replSetGetStatus-primary.bson"
	testReplSetStatusSecondaryFile = "testdata/replSetGetStatus-secondary.bson"
)

var (
	testSecondary2Host = testutils.MongoDBHost + ":" + testutils.MongoDBSecondary2Port
)

func loadBSONFile(file string, out interface{}) error {
	bytes, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	return bson.Unmarshal(bytes, out)
}

func TestScoreReplset(t *testing.T) {
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
	}

	status, err := rs.GetStatus()
	if err != nil {
		t.Fatalf("Failed to run .GetStatus() on Replset struct: %v", err.Error())
	}

	scorer, err := ScoreReplset(config, status, nil)
	if err != nil {
		t.Fatalf("Failed to run .ScoreReplset(): %v", err.Error())
	} else if len(scorer.members) < 1 {
		t.Fatal("Got zero scored members from .ScoreReplset()")
	}

	winner := scorer.Winner()
	if winner == nil {
		t.Fatal(".Winner() returned nil")
	}

	if winner.Name() != testSecondary2Host {
		t.Fatalf("Expected .Winner() to return host %v, not %v", testSecondary2Host, winner.Name())
	}

	// make sure .ScoreReplset() returns the same winner consistently when
	// using the Primary OR Secondary for the source of the replset status
	// (replSetGetStatus)
	getConfig := &mdbstructs.ReplSetGetConfig{}
	err = loadBSONFile(testReplSetConfigFile, &getConfig)
	if err != nil {
		t.Fatalf("Cannot load test file %v: %v", testReplSetConfigFile, err.Error())
	}
	for _, statusFile := range []string{testReplSetStatusPrimaryFile, testReplSetStatusSecondaryFile} {
		status := &mdbstructs.ReplsetStatus{}
		err = loadBSONFile(statusFile, &status)
		if err != nil {
			t.Fatalf("Cannot load test file %v: %v", statusFile, err.Error())
		}

		scorer, err := ScoreReplset(getConfig.Config, status, nil)
		if err != nil {
			t.Fatalf("Failed to run .ScoreReplset(): %v", err.Error())
		}

		winner := scorer.Winner()
		if winner == nil {
			t.Fatal(".Winner() returned nil")
		}

		if winner.Name() != testSecondary2Host {
			t.Fatalf("Expected .Winner() to return host %v, not %v", testSecondary2Host, winner.Name())
		}
	}
}
