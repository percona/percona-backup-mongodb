package cluster

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/mdbstructs"
)

const (
	baseScore               = 100
	hiddenMemberMultiplier  = +0.3
	primaryMemberMultiplier = -0.5
	priorityZeroMultiplier  = +0.1
	priorityWeight          = 1.0
)

var (
	replsetReadPreference = mgo.PrimaryPreferred
)

//func HasReplsetTag(config *mdbstructs.ReplsetConfig, key, val string) bool {
//}

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

type ScoringMember struct {
	name   string
	config *mdbstructs.ReplsetConfigMember
	status *mdbstructs.ReplsetStatusMember
	score  float64
	scored bool
	log    []string
}

func (sm *ScoringMember) GetScore() float64 {
	return sm.score
}

func (sm *ScoringMember) SetScore(score float64, msg string) {
	sm.scored = true
	sm.score = score
	sm.log = append(sm.log, msg)
}

func (sm *ScoringMember) MultiplyScore(multiplier float64, msg string) {
	sm.scored = true
	sm.score += (sm.score * multiplier)
	sm.log = append(sm.log, msg)
}

func (sm *ScoringMember) AddScore(add float64, msg string) {
	sm.scored = true
	sm.score += add
	sm.log = append(sm.log, msg)
}

func ScoreMembers(config *mdbstructs.ReplsetConfig, status *mdbstructs.ReplsetStatus, tags *mdbstructs.ReplsetTags) (map[string]*ScoringMember, error) {
	members := make(map[string]*ScoringMember)
	for _, cnfMember := range config.Members {
		var statusMember *mdbstructs.ReplsetStatusMember
		for _, m := range status.Members {
			if m.Name == cnfMember.Host {
				statusMember = m
				break
			}
		}
		if statusMember == nil {
			return nil, errors.New("no status info")
		}

		member := &ScoringMember{
			config: cnfMember,
			status: statusMember,
			score:  baseScore,
		}
		if statusMember.Health != mdbstructs.ReplsetMemberHealthUp {
			member.SetScore(0, "member is down")
		}

		if statusMember.State == mdbstructs.ReplsetMemberStatePrimary {
			member.MultiplyScore(primaryMemberMultiplier, "member is primary")
		} else if statusMember.State != mdbstructs.ReplsetMemberStateSecondary {
			member.SetScore(0, "member is not secondary or primary")
		}

		if cnfMember.Hidden == true {
			member.MultiplyScore(hiddenMemberMultiplier, "member is hidden")
		} else if cnfMember.Priority == 0 {
			member.MultiplyScore(priorityZeroMultiplier, "member has priority == 0")
		} else if cnfMember.Priority > 1 {
			addScore := float64(cnfMember.Priority-1) * priorityWeight
			member.AddScore(addScore*-1, "member has priority > 1")
		}

		members[member.name] = member
		fmt.Printf("%v\n", member)
	}
	return members, nil
}
