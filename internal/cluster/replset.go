package cluster

import (
	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/mdbstructs"
)

//func HasReplsetTag(config *mdbstructs.ReplsetConfig, key, val string) bool {
//}

func GetConfig(session *mgo.Session) (*mdbstructs.ReplsetConfig, error) {
	rsGetConfig := mdbstructs.ReplSetGetConfig{}
	err := session.Run(bson.D{{"replSetGetConfig", "1"}}, &rsGetConfig)
	return rsGetConfig.Config, err
}

func GetBackupSource(config *mdbstructs.ReplsetConfig, status *mdbstructs.ReplsetStatus) (*mdbstructs.ReplsetConfigMember, error) {
	scorer, err := ScoreReplset(config, status, nil)
	if err != nil {
		return nil, err
	}
	return scorer.Winner().config, nil
}
