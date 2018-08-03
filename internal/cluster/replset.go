package cluster

import (
	"time"

	"github.com/globalsign/mgo"
	rsConfig "github.com/timvaillancourt/go-mongodb-replset/config"
	rsStatus "github.com/timvaillancourt/go-mongodb-replset/status"
)

const (
	hiddenMemberWeight = 0.2
	priorityZeroWeight = 0.1
)

var (
	replsetReadPreference = mgo.PrimaryPreferred
)

type Replset struct {
	name  string
	addrs []string
}

func (r *Replset) GetSession(username, password string) (*mgo.Session, error) {
	session, err := mgo.DialWithInfo(&mgo.DialInfo{
		Addrs:          r.addrs,
		Username:       username,
		Password:       password,
		ReplicaSetName: r.name,
		FailFast:       true,
		Timeout:        10 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	session.SetMode(replsetReadPreference, true)
	return session, nil
}

func (r *Replset) GetConfig() (*rsConfig.Config, error) {
	return nil, nil
}

func (r *Replset) GetStatus() (*rsStatus.Status, error) {
	return nil, nil
}

func getBackupNode(config *rsConfig.Config) (*rsConfig.Member, error) {
	return config.Members[0], nil
}
