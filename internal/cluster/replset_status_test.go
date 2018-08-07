package cluster

import (
	"testing"
	"time"

	"github.com/globalsign/mgo/bson"
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
	now := time.Now()
	lastHB := now.Add(-10 * time.Second)
	primaryTs, _ := bson.NewMongoTimestamp(now, 0)
	secondaryTs, _ := bson.NewMongoTimestamp(now.Add(-15*time.Second), 1)
	status := mdbstructs.ReplsetStatus{
		Date: now,
		Members: []*mdbstructs.ReplsetStatusMember{
			{
				Id:                0,
				Name:              "test:27017",
				Optime:            &mdbstructs.OpTime{Ts: primaryTs},
				State:             mdbstructs.ReplsetMemberStatePrimary,
				LastHeartbeatRecv: lastHB,
				LastHeartbeat:     lastHB.Add(-150 * time.Millisecond),
			},
			{
				Id:     1,
				Name:   "test:27018",
				Optime: &mdbstructs.OpTime{Ts: secondaryTs},
				State:  mdbstructs.ReplsetMemberStateSecondary,
				Self:   true,
			},
			{
				Id:                2,
				Name:              "test:27019",
				Optime:            &mdbstructs.OpTime{Ts: secondaryTs},
				State:             mdbstructs.ReplsetMemberStateSecondary,
				LastHeartbeatRecv: lastHB,
				LastHeartbeat:     lastHB.Add(-100 * time.Millisecond),
			},
		},
	}

	// test the lag is 14.95 seconds
	lag, err := GetReplsetLagDuration(&status, GetReplsetStatusMember(&status, "test:27019"))
	if err != nil {
		t.Fatalf("Could not get lag: %v", err.Error())
	}
	if lag.Nanoseconds() != 14950000000 {
		t.Fatalf("Lag should be 14.95s, got: %v", lag)
	}

	// test the lag is 4.85 seconds
	status.Members[0].Optime.Ts = secondaryTs
	status.Members[1].Optime.Ts = primaryTs
	lag, err = GetReplsetLagDuration(&status, GetReplsetStatusMember(&status, "test:27018"))
	if err != nil {
		t.Fatalf("Could not get lag: %v", err.Error())
	}
	if lag.Nanoseconds() != 4850000000 {
		t.Fatalf("Lag should be 4.85s, got: %v", lag)
	}
}
