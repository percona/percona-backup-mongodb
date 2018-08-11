package cluster

import (
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/mdbstructs"
)

var (
	replsetReadPreference = mgo.PrimaryPreferred
)

//func HasReplsetTag(config *mdbstructs.ReplsetConfig, key, val string) bool {
//}

type Replset struct {
	name   string
	addrs  []string
	config *Config
}

func NewReplset(config *Config, name string, addrs []string) *Replset {
	return &Replset{
		name:   name,
		addrs:  addrs,
		config: config,
	}
}

func (r *Replset) GetReplsetSession() (*mgo.Session, error) {
	session, err := mgo.DialWithInfo(&mgo.DialInfo{
		Addrs:          r.addrs,
		Username:       r.config.Username,
		Password:       r.config.Password,
		ReplicaSetName: r.name,
		Timeout:        10 * time.Second,
	})
	if err != nil || session.Ping() != nil {
		return session, err
	}
	session.SetMode(replsetReadPreference, true)
	return session, err
}

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
