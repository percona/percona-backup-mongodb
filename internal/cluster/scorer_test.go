package cluster

import (
	"testing"

	"github.com/percona/mongodb-backup/internal/testutils"
)

func TestScorerRun(t *testing.T) {
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

	scorer, err := NewScorer(config, status, nil)
	if err != nil {
		t.Fatalf("Could not init Scorer: %v", err.Error())
	}

	err = scorer.Score()
	if err != nil {
		t.Fatalf("Failed to run Scorer .Run(): %v", err.Error())
	} else if len(scorer.members) < 1 {
		t.Fatal("Got zero scored members from Scorer .Run()")
	}
}
