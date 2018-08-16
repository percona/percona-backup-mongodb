package cluster

import (
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils"
	"github.com/percona/mongodb-backup/mdbstructs"
)

const (
	testReplSetConfigFile          = "testdata/replSetGetConfig.bson"
	testReplSetStatusPrimaryFile   = "testdata/replSetGetStatus-primary.bson"
	testReplSetStatusSecondaryFile = "testdata/replSetGetStatus-secondary.bson"
)

func TestScoreReplset(t *testing.T) {
	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Could not connect to replset: %v", err.Error())
	}
	defer session.Close()

	config, err := GetConfig(session)
	if err != nil {
		t.Fatalf("Failed to run .GetConfig() on Replset struct: %v", err.Error())
	}

	status, err := GetStatus(session)
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

	expectScore := 217
	if winner.Name() != testSecondary2Host {
		t.Fatalf("Expected .Winner() to return host %v, not %v", testSecondary2Host, winner.Name())
	} else if winner.Score() != expectScore {
		t.Fatalf("Expected .Winner() to return host %d, not %v", expectScore, winner.Score())
	}

	// test w/replset tags
	scorer, err = ScoreReplset(config, status, map[string]string{"role": "backup"})
	if err != nil {
		t.Fatalf("Failed to run .ScoreReplset(): %v", err.Error())
	}

	winner = scorer.Winner()
	if winner == nil {
		t.Fatal(".Winner() returned nil")
	}

	expectScore = 391
	if winner.Name() != testSecondary2Host {
		t.Fatalf("Expected .Winner() to return host %v, not %v", testSecondary2Host, winner.Name())
	} else if winner.Score() != expectScore {
		t.Fatalf("Expected .Winner() to return host %d, not %v", expectScore, winner.Score())
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
