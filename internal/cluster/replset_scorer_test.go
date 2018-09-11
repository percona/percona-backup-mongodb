package cluster

import (
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils"
	"github.com/percona/mongodb-backup/internal/testutils/db"
	"github.com/percona/mongodb-backup/mdbstructs"
)

const (
	testReplSetConfigFile          = "testdata/replSetGetConfig.bson"
	testReplSetStatusPrimaryFile   = "testdata/replSetGetStatus-primary.bson"
	testReplSetStatusSecondaryFile = "testdata/replSetGetStatus-secondary.bson"
)

func TestReplsetScoreMembers(t *testing.T) {
	session, err := mgo.DialWithInfo(db.PrimaryDialInfo(t, testutils.MongoDBShard1ReplsetName))
	if err != nil {
		t.Fatalf("Could not connect to replset: %v", err.Error())
	}
	defer session.Close()

	r, err := NewReplset(session)
	if err != nil {
		t.Fatalf("Failed to run .NewReplset(): %v", err.Error())
	}

	winner, err := r.BackupSource(nil)
	if err != nil {
		t.Fatalf("Cannot get backup source: %s", err)
	}
	if winner == "" {
		t.Errorf("Got empty backup source")
	}

	if winner != testSecondary2Host {
		t.Fatalf("Expected .Winner() to return host %v, not %v", testSecondary2Host, winner)
	}

	// test w/replset tags
	winner, err = r.BackupSource(map[string]string{"role": "backup"})
	if err != nil {
		t.Fatalf("Cannot get backup source: %s", err)
	}
	if winner != testSecondary2Host {
		t.Fatalf("Expected .Winner() to return host %v, not %v", testSecondary2Host, winner)
	}

	// make sure .score() returns the same winner consistently when
	// using the Primary OR Secondary for the source of the replset status
	// (replSetGetStatus)
	getConfig := &mdbstructs.ReplSetGetConfig{}
	err = loadBSONFile(testReplSetConfigFile, &getConfig)
	if err != nil {
		t.Fatalf("Cannot load test file %v: %v", testReplSetConfigFile, err.Error())
	}
	for _, statusFile := range []string{testReplSetStatusPrimaryFile, testReplSetStatusSecondaryFile} {
		err = loadBSONFile(statusFile, r.status)
		if err != nil {
			t.Fatalf("Cannot load test file %v: %v", statusFile, err.Error())
		}

		winner, err = r.BackupSource(map[string]string{"role": "backup"})
		if err != nil {
			t.Fatalf("Cannot get backup source: %s", err)
		}
		if winner != testSecondary2Host {
			t.Fatalf("Expected .Winner() to return host %v, not %v", testSecondary2Host, winner)
		}

	}
}
