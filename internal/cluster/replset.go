package cluster

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/mdbstructs"
)

// HasReplsetMemberTags returns a boolean reflecting whether or not
// a replica set config member matches a list of replica set tags
//
// https://docs.mongodb.com/manual/reference/replica-configuration/#rsconf.members[n].tags
//
func HasReplsetMemberTags(member *mdbstructs.ReplsetConfigMember, tags map[string]string) bool {
	if len(member.Tags) == 0 || len(tags) == 0 {
		return false
	}
	for key, val := range tags {
		if tagVal, ok := member.Tags[key]; ok {
			if tagVal != val {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

// GetConfig returns a struct representing the "replSetGetConfig" server
// command
//
// https://docs.mongodb.com/manual/reference/command/replSetGetConfig/
//
func GetConfig(session *mgo.Session) (*mdbstructs.ReplsetConfig, error) {
	rsGetConfig := mdbstructs.ReplSetGetConfig{}
	err := session.Run(bson.D{{"replSetGetConfig", "1"}}, &rsGetConfig)
	return rsGetConfig.Config, err
}

// GetStatus returns a struct representing the "replSetGetStatus" server
// command
//
// https://docs.mongodb.com/manual/reference/command/replSetGetStatus/
//
func GetStatus(session *mgo.Session) (*mdbstructs.ReplsetStatus, error) {
	status := mdbstructs.ReplsetStatus{}
	err := session.Run(bson.D{{"replSetGetStatus", "1"}}, &status)
	return &status, err
}

// GetReplsetName returns the replica set name as a string
func GetReplsetName(config *mdbstructs.ReplsetConfig) string {
	return config.Name
}

// GetReplsetID returns the replica set ID as a bson.ObjectId
func GetReplsetID(config *mdbstructs.ReplsetConfig) *bson.ObjectId {
	return &config.Settings.ReplicaSetId
}

// GetBackupSource returns the the most appropriate replica set member
// to become the source of the backup. The chosen node should cause
// the least impact/risk possible during backup
func GetBackupSource(config *mdbstructs.ReplsetConfig, status *mdbstructs.ReplsetStatus) (*mdbstructs.ReplsetConfigMember, error) {
	scorer, err := ScoreReplset(config, status, nil)
	if err != nil {
		return nil, err
	}
	return scorer.Winner().config, nil
}
