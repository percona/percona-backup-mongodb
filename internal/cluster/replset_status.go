package cluster

import (
	"errors"
	"time"

	"github.com/percona/mongodb-backup/mdbstructs"
)

// positiveDuration converts a negative duration to a positive duration,
// if the duration is negative
func positiveDuration(duration time.Duration) time.Duration {
	if duration < 0 {
		return duration * -1
	}
	return duration
}

// getMemberOpLag returns the operational latency of MongoDB Replica Set
// heartbeats as a time duration
func getMemberOpLag(status *mdbstructs.ReplsetStatus, member *mdbstructs.ReplsetStatusMember) time.Duration {
	heartbeatLag := member.LastHeartbeatRecv.Sub(member.LastHeartbeat)
	heartbeatAge := status.Date.Sub(member.LastHeartbeatRecv)
	return heartbeatLag + heartbeatAge
}

// GetReplsetLagDuration returns the lag between the replica set Primary
// and the provided member host by considering the latency in Replica Set
// heartbeats and oplog timestamps of members
func GetReplsetLagDuration(status *mdbstructs.ReplsetStatus, compare *mdbstructs.ReplsetStatusMember) (time.Duration, error) {
	var lag time.Duration
	var opLag time.Duration

	primary := GetReplsetStatusPrimary(status)
	if primary == nil {
		return lag, errors.New("no primary")
	} else if compare == nil {
		return lag, errors.New("no compare member")
	} else if primary.Name == compare.Name {
		return lag, nil
	}

	var primaryOpLag time.Duration
	var compareOpLag time.Duration
	if !primary.Self {
		primaryOpLag = getMemberOpLag(status, primary)
	}
	if !compare.Self {
		compareOpLag = getMemberOpLag(status, compare)
	}

	if !primary.Self && !compare.Self {
		opLag = positiveDuration(primaryOpLag - compareOpLag)
	} else if primary.Self {
		opLag = compareOpLag
	} else if compare.Self {
		opLag = primaryOpLag
	}

	primaryTs := primary.Optime.Ts.Time()
	compareTs := compare.Optime.Ts.Time()
	lag = positiveDuration(compareTs.Sub(primaryTs))
	if lag > opLag {
		lag -= opLag
	}

	return lag, nil
}

// GetReplsetStatusMember returns the status for a replica set  member, by host
// name using the output of the MongoDB 'replSetGetStatus' server command.
func GetReplsetStatusMember(status *mdbstructs.ReplsetStatus, host string) *mdbstructs.ReplsetStatusMember {
	for _, member := range status.Members {
		if member.Name == host {
			return member
		}
	}
	return nil
}

// GetReplsetStatusMember returns the status for the replica set Primary member
// using the output of the MongoDB 'replSetGetStatus' server command.
func GetReplsetStatusPrimary(status *mdbstructs.ReplsetStatus) *mdbstructs.ReplsetStatusMember {
	for _, member := range status.Members {
		if member.State == mdbstructs.ReplsetMemberStatePrimary {
			return member
		}
	}
	return nil
}
