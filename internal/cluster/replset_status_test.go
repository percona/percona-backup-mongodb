package cluster

import (
	"testing"
	"time"

	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/mdbstructs"
)

func TestReplsetGetLagDuration(t *testing.T) {
	now := time.Now()
	lastHB := now.Add(-10 * time.Second)
	primaryTs, _ := bson.NewMongoTimestamp(now, 0)
	secondaryTs, _ := bson.NewMongoTimestamp(now.Add(-15*time.Second), 1)
	status := mdbstructs.ReplsetStatus{
		Date: now,
		Members: []*mdbstructs.ReplsetStatusMember{
			{
				Name:              "test:27017",
				Optime:            &mdbstructs.OpTime{Ts: primaryTs},
				State:             mdbstructs.ReplsetMemberStatePrimary,
				LastHeartbeatRecv: lastHB,
				LastHeartbeat:     lastHB.Add(-150 * time.Millisecond),
			},
			{
				Name:   "test:27018",
				Optime: &mdbstructs.OpTime{Ts: secondaryTs},
				State:  mdbstructs.ReplsetMemberStateSecondary,
				Self:   true,
			},
			{
				Name:              "test:27019",
				Optime:            &mdbstructs.OpTime{Ts: secondaryTs},
				State:             mdbstructs.ReplsetMemberStateSecondary,
				LastHeartbeatRecv: lastHB,
				LastHeartbeat:     lastHB.Add(-100 * time.Millisecond),
			},
		},
	}

	// test the lag is 14.95 seconds for secondary (both secondary and primary not-self)
	r := &Replset{status: &status}
	lag, err := r.getLagDuration(r.GetStatusMember("test:27019"))
	if err != nil {
		t.Fatalf("Could not get lag: %v", err.Error())
	}
	if lag.Nanoseconds() != 14950000000 {
		t.Fatalf("Lag should be 14.95s, got: %v", lag)
	}

	// test the lag is 4.85 seconds (secondary is self)
	r.status.Members[0].Optime.Ts = secondaryTs
	r.status.Members[1].Optime.Ts = primaryTs
	lag, err = r.getLagDuration(r.GetStatusMember("test:27018"))
	if err != nil {
		t.Fatalf("Could not get lag: %v", err.Error())
	}
	if lag.Nanoseconds() != 4850000000 {
		t.Fatalf("Lag should be 4.85s, got: %v", lag)
	}

	// test lag is 0 seconds (compare host is primary - always 0s)
	r.status.Members[0].State = mdbstructs.ReplsetMemberStateSecondary
	r.status.Members[1].State = mdbstructs.ReplsetMemberStatePrimary
	lag, err = r.getLagDuration(r.GetStatusMember("test:27018"))
	if err != nil {
		t.Fatalf("Could not get lag: %v", err.Error())
	}
	if lag.Seconds() != 0 {
		t.Fatalf("Lag should be 0s, got: %v", lag)
	}

	// test lag is 4.9 seconds (primary is self)
	lag, err = r.getLagDuration(r.GetStatusMember("test:27019"))
	if err != nil {
		t.Fatalf("Could not get lag: %v", err.Error())
	}
	if lag.Nanoseconds() != 4900000000 {
		t.Fatalf("Lag should be 4.90s, got: %v", lag)
	}
}
