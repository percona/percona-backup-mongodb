package cluster

import (
	"errors"
	"fmt"

	"github.com/percona/mongodb-backup/mdbstructs"
)

const (
	baseScore               = 100
	hiddenMemberMultiplier  = +0.3
	primaryMemberMultiplier = -0.5
	priorityZeroMultiplier  = +0.1
	priorityWeight          = 1.0
	votesWeight             = 2.0
)

type ScoringMember struct {
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

		if cnfMember.Votes > 1 {
			addScore := float64(cnfMember.Votes-1) * votesWeight
			member.AddScore(addScore*-1, "member has votes > 1")
		}

		s.members[cnfMember.Host] = member
		fmt.Printf("%v\n", member)
	}
	return nil
}
