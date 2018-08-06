package cluster

import (
	"time"

	"github.com/globalsign/mgo"
	rsConfig "github.com/timvaillancourt/go-mongodb-replset/config"
	rsStatus "github.com/timvaillancourt/go-mongodb-replset/status"
	mgov2 "gopkg.in/mgo.v2"
)

const (
	hiddenMemberWeight = 0.2
	priorityZeroWeight = 0.1
)

var (
	replsetReadPreference = mgo.PrimaryPreferred
)

type Replset struct {
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
	var err error
	r.session, err = mgo.DialWithInfo(&mgo.DialInfo{
		Addrs:          r.addrs,
		Username:       r.username,
		Password:       r.password,
		ReplicaSetName: r.name,
		Timeout:        10 * time.Second,
	})
	if err != nil {
		return err
	}
	r.session.SetMode(replsetReadPreference, true)
	return nil
}

func (r *Replset) GetConfig() (*rsConfig.Config, error) {
	manager := rsConfig.New(r.session.(*mgov2.Session))
	err := manager.Load()
	return manager.Get(), err
}

func (r *Replset) GetStatus() (*rsStatus.Status, error) {
	return rsStatus.New(r.session)
}

func getBackupNode(config *rsConfig.Config) (*rsConfig.Member, error) {
	return config.Members[0], nil
}
