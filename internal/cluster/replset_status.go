package cluster

import (
	"time"

	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/mdbstructs"
)

// GetReplsetLagDuration returns the lag between the
// replica set Primary and the provided member host.
func GetReplsetLagDuration(status *mdbstructs.ReplsetStatus, compareHost string) (time.Duration, error) {
	return time.Second, nil
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

func (r *Replset) GetStatus() (*mdbstructs.ReplsetStatus, error) {
	status := mdbstructs.ReplsetStatus{}
	err := r.session.Run(bson.D{{"replSetGetStatus", "1"}}, &status)
	return &status, err
}
