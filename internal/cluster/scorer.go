package cluster

import (
	"errors"
	"fmt"
	"time"

	"github.com/percona/mongodb-backup/mdbstructs"
)

const (
	baseScore                 = 100
	hiddenMemberMultiplier    = 1.5
	secondaryMemberMultiplier = 1.2
	priorityZeroMultiplier    = 1.2
	replsetOkLagMultiplier    = 1.1
)

var (
	maxOkReplsetLagDuration = 30 * time.Second
	maxReplsetLagDuration   = 5 * time.Minute
)

type ScorerMsg string

const (
	msgMemberDown            ScorerMsg = "member is down"
	msgMemberSecondary       ScorerMsg = "member is secondary"
	msgMemberBadState        ScorerMsg = "member has bad state"
	msgMemberHidden          ScorerMsg = "member is hidden"
	msgMemberPriorityZero    ScorerMsg = "member has priority 0"
	msgMemberPriorityNonZero ScorerMsg = "member has non-zero priority"
	msgMemberReplsetLagOk    ScorerMsg = "member has ok replset lag"
	msgMemberReplsetLagFail  ScorerMsg = "member has high replset lag"
	msgMemberVotesGtOne      ScorerMsg = "member has votes > 1"
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
	sm.score *= multiplier
	sm.log = append(sm.log, msg)
}

func (sm *ScoringMember) AddScore(add float64, msg ScorerMsg) {
	sm.score += add
	sm.log = append(sm.log, msg)
}

type Scorer struct {
	status  *mdbstructs.ReplsetStatus
	tags    *mdbstructs.ReplsetTags
	members map[string]*ScoringMember
}

func NewScorer(config *mdbstructs.ReplsetConfig, status *mdbstructs.ReplsetStatus, tags *mdbstructs.ReplsetTags) (*Scorer, error) {
	scorer := &Scorer{
		status: status,
		tags:   tags,
	}
	var err error
	scorer.members, err = getScoringMembers(config, status)
	return scorer, err
}

func (s *Scorer) minPrioritySecondary() *ScoringMember {
	var minPriority *ScoringMember
	for _, member := range s.members {
		if member.status.State != mdbstructs.ReplsetMemberStateSecondary {
			continue
		}
		if minPriority == nil || member.config.Priority < minPriority.config.Priority {
			minPriority = member
		}
	}
	return minPriority
}

func (s *Scorer) minVotesSecondary() *ScoringMember {
	var minVotes *ScoringMember
	for _, member := range s.members {
		if member.status.State != mdbstructs.ReplsetMemberStateSecondary {
			continue
		}
		if minVotes == nil || member.config.Votes < minVotes.config.Votes {
			minVotes = member
		}
	}
	return minVotes
}

func getScoringMembers(config *mdbstructs.ReplsetConfig, status *mdbstructs.ReplsetStatus) (map[string]*ScoringMember, error) {
	members := map[string]*ScoringMember{}
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
		members[cnfMember.Host] = &ScoringMember{
			config: cnfMember,
			status: statusMember,
			score:  baseScore,
		}
	}
	return members, nil
}

func (s *Scorer) Score() error {
	for _, member := range s.members {
		// health
		if member.status.Health != mdbstructs.ReplsetMemberHealthUp {
			member.SetScore(0, msgMemberDown)
			s.members[member.config.Host] = member
			continue
		}

		// state
		if member.status.State == mdbstructs.ReplsetMemberStateSecondary {
			member.MultiplyScore(secondaryMemberMultiplier, msgMemberSecondary)
		} else if member.status.State != mdbstructs.ReplsetMemberStatePrimary {
			member.SetScore(0, msgMemberBadState)
			s.members[member.config.Host] = member
			continue
		}

		// votes
		//if member.config.Votes > 1 {
		//	addScore := (1 - float64(member.config.Votes/maxConfigVotes)) * 100
		//	member.AddScore(addScore, msgMemberVotesGtOne)
		//}

		// secondary only
		if member.status.State == mdbstructs.ReplsetMemberStateSecondary {
			if member.config.Hidden {
				member.MultiplyScore(hiddenMemberMultiplier, msgMemberHidden)
			} else if member.config.Priority == 0 {
				member.MultiplyScore(priorityZeroMultiplier, msgMemberPriorityZero)
				//} else if member.config.Priority >= 1 {
				//	addScore := (1 - float64(member.config.Priority/maxConfigPriority)) //* 100
				//	member.AddScore(addScore, msgMemberPriorityNonZero)
			}

			replsetLag, err := GetReplsetLagDuration(s.status, GetReplsetStatusMember(s.status, member.config.Host))
			if err != nil {
				return err
			}
			if replsetLag < maxOkReplsetLagDuration {
				member.MultiplyScore(replsetOkLagMultiplier, msgMemberReplsetLagOk)
			} else if replsetLag >= maxReplsetLagDuration {
				member.SetScore(0, msgMemberReplsetLagFail)
			}
		}

		fmt.Printf("%v %v %v\n", member.config.Host, member.GetScore(), member.log)
	}
	return nil
}
