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

// GetReplsetLag returns the lag between the replica set
// Primary and the provided member host.
func GetReplsetLag(status *mdbstructs.ReplsetStatus, compareHost string) (time.Duration, error) {
	return time.Second, nil
}

type Replset struct {
	sync.Mutex
	name     string
	addrs    []string
	username string
	password string
	session  *mgo.Session
}

func NewReplset(name string, addrs []string, username, password string) (*Replset, error) {
	r := &Replset{
		name:     name,
		addrs:    addrs,
		username: username,
		password: password,
	}
	return r, r.getSession()
}

func (r *Replset) getSession() error {
	r.Lock()
	defer r.Unlock()

	var err error
	r.session, err = mgo.DialWithInfo(&mgo.DialInfo{
		Addrs:          r.addrs,
		Username:       r.username,
		Password:       r.password,
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

func (r *Replset) GetStatus() (*mdbstructs.ReplsetStatus, error) {
	status := mdbstructs.ReplsetStatus{}
	err := r.session.Run(bson.D{{"replSetGetStatus", "1"}}, &status)
	return &status, err
}
