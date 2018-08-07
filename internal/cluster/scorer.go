package cluster

import (
	"errors"
	"fmt"
	"time"

	"github.com/percona/mongodb-backup/mdbstructs"
)

const (
	baseScore = 100

	hiddenMemberMultiplier  = +0.3
	primaryMemberMultiplier = -0.5
	priorityZeroMultiplier  = +0.1
	replsetMaxLagMultiplier = +0.2

	priorityWeight = 1.0
	votesWeight    = 2.0
)

var (
	maxReplsetLagDuration = time.Minute
)

type ScorerMsg string

const (
	msgMemberDown          ScorerMsg = "member is down"
	msgMemberPrimary       ScorerMsg = "member is primary"
	msgMemberBadState      ScorerMsg = "member has bad state"
	msgMemberHidden        ScorerMsg = "member is hidden"
	msgMemberPriorityZero  ScorerMsg = "member has priority 0"
	msgMemberPriorityGtOne ScorerMsg = "member has priority > 1"
	msgMemberReplsetLag    ScorerMsg = "member has replset lag"
	msgMemberVotesGtOne    ScorerMsg = "member has votes > 1"
)

type ScoringMember struct {
	config *mdbstructs.ReplsetConfigMember
	status *mdbstructs.ReplsetStatusMember
	score  float64
	log    []ScorerMsg
}

func (sm *ScoringMember) GetScore() float64 {
	return sm.score
}

func (sm *ScoringMember) SetScore(score float64, msg ScorerMsg) {
	sm.score = score
	sm.log = append(sm.log, msg)
}

func (sm *ScoringMember) MultiplyScore(multiplier float64, msg ScorerMsg) {
	sm.score += (sm.score * multiplier)
	sm.log = append(sm.log, msg)
}

func (sm *ScoringMember) AddScore(add float64, msg ScorerMsg) {
	sm.score += add
	sm.log = append(sm.log, msg)
}

type Scorer struct {
	config  *mdbstructs.ReplsetConfig
	status  *mdbstructs.ReplsetStatus
	tags    *mdbstructs.ReplsetTags
	members map[string]*ScoringMember
}

func NewScorer(config *mdbstructs.ReplsetConfig, status *mdbstructs.ReplsetStatus, tags *mdbstructs.ReplsetTags) *Scorer {
	return &Scorer{
		config:  config,
		status:  status,
		tags:    tags,
		members: make(map[string]*ScoringMember),
	}
}

func (s *Scorer) Score() error {
	for _, cnfMember := range s.config.Members {
		var statusMember *mdbstructs.ReplsetStatusMember
		for _, m := range s.status.Members {
			if m.Name == cnfMember.Host {
				statusMember = m
				break
			}
		}
		if statusMember == nil {
			return errors.New("no status info")
		}

		member := &ScoringMember{
			config: cnfMember,
			status: statusMember,
			score:  baseScore,
		}

		// health
		if statusMember.Health != mdbstructs.ReplsetMemberHealthUp {
			member.SetScore(0, msgMemberDown)
		}

		// state
		if statusMember.State == mdbstructs.ReplsetMemberStatePrimary {
			member.MultiplyScore(primaryMemberMultiplier, msgMemberPrimary)
		} else if statusMember.State != mdbstructs.ReplsetMemberStateSecondary {
			member.SetScore(0, msgMemberBadState)
		}

		// hidden/priority
		if cnfMember.Hidden {
			member.MultiplyScore(hiddenMemberMultiplier, msgMemberHidden)
		} else if cnfMember.Priority == 0 {
			member.MultiplyScore(priorityZeroMultiplier, msgMemberPriorityZero)
		} else if cnfMember.Priority > 1 {
			addScore := float64(cnfMember.Priority-1) * priorityWeight
			member.AddScore(addScore*-1, msgMemberPriorityGtOne)
		}

		// votes
		if cnfMember.Votes > 1 {
			addScore := float64(cnfMember.Votes-1) * votesWeight
			member.AddScore(addScore*-1, msgMemberVotesGtOne)
		}

		// replset lag
		replsetLag, err := GetReplsetLagDuration(s.status, GetReplsetStatusMember(s.status, s.config.Name))
		if err != nil {
			return err
		}
		if replsetLag > maxReplsetLagDuration {
			member.MultiplyScore(replsetMaxLagMultiplier, msgMemberReplsetLag)
		}

		s.members[cnfMember.Host] = member
		fmt.Printf("%v %v %v\n", cnfMember.Host, member.GetScore(), member.log)
	}
	return nil
}
