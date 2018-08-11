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

func NewReplset(config *Config, name string, addrs []string) (*Replset, error) {
	r := &Replset{
		name:   name,
		addrs:  addrs,
		config: config,
	}
	return r, r.getSession()
}

func GetReplsetSession() (*mgo.Session, error) {
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
	err := r.session.Run(bson.D{{"replSetGetConfig", "1"}}, &rsGetConfig)
	return rsGetConfig.Config, err
}

func GetBackupSource(session *mgo.Session) (*mdbstructs.ReplsetConfigMember, error) {
	config, err := GetConfig(session)
	if err != nil {
		return nil, err
	}
	status, err := GetStatus(session)
	if err != nil {
		return nil, err
	}
	scorer, err := ScoreReplset(config, status, nil)
	if err != nil {
		return nil, err
	}
	return scorer.Winner().config, nil
}
