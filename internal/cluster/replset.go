package cluster

import (
	"sync"
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
	sync.Mutex
	name    string
	addrs   []string
	config  *Config
	session *mgo.Session
	scorer  *ReplsetScorer
}

func NewReplset(config *Config, name string, addrs []string) (*Replset, error) {
	r := &Replset{
		name:   name,
		addrs:  addrs,
		config: config,
	}
	return r, r.getSession()
}

func (r *Replset) getSession() error {
	r.Lock()
	defer r.Unlock()

	var err error
	r.session, err = mgo.DialWithInfo(&mgo.DialInfo{
		Addrs:          r.addrs,
		Username:       r.config.Username,
		Password:       r.config.Password,
		ReplicaSetName: r.name,
		Timeout:        10 * time.Second,
	})
	if err != nil || r.session.Ping() != nil {
		return err
	}
	r.session.SetMode(replsetReadPreference, true)
	return nil
}

func (r *Replset) Close() {
	r.Lock()
	defer r.Unlock()

	if r.session != nil {
		r.session.Close()
	}
}

func (r *Replset) GetConfig() (*mdbstructs.ReplsetConfig, error) {
	rsGetConfig := mdbstructs.ReplSetGetConfig{}
	err := r.session.Run(bson.D{{"replSetGetConfig", "1"}}, &rsGetConfig)
	return rsGetConfig.Config, err
}

func (r *Replset) GetBackupSource() (*mdbstructs.ReplsetConfigMember, error) {
	config, err := r.GetConfig()
	if err != nil {
		return nil, err
	}
	status, err := r.GetStatus()
	if err != nil {
		return nil, err
	}
	scorer, err := ScoreReplset(config, status, nil)
	if err != nil {
		return nil, err
	}
	return scorer.Winner().config, nil
}
