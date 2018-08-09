package cluster

import (
	"testing"

	"github.com/percona/mongodb-backup/internal/testutils"
)

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

	if winner.Name() != "127.0.0.1:17003" {
		t.Fatalf("Expected .Winner() to return host 127.0.0.1:17003, not %v", winner.Name())
	}
}
