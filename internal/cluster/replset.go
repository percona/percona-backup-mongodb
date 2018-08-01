package cluster

import (
	"time"

	"github.com/globalsign/mgo"
	rsConfig "github.com/timvaillancourt/go-mongodb-replset/config"
)

const (
	hiddenMemberMultiplier = 1.0
)

var (
	replsetReadPreference = mgo.PrimaryPreferred
)

type Shard struct {
	name    string
	uri     string
	replset *Replset
}

func NewShard(name, uri string) *Shard {
	s := &Shard{
		name:    name,
		uri:     uri,
		replset: &Replset{name: name},
	}
	s.replset.addrs = s.Addrs()
	return s
}

func (s *Shard) Addrs() []string {
	return []string{"127.0.0.1:27017"}
}

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

func getBackupNode(config *rsConfig.Config) (*rsConfig.Member, error) {
	return config.Members[0], nil
}
