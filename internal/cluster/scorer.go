package cluster

import (
	"errors"
	"fmt"
	"time"

	//"github.com/davecgh/go-spew/spew"
	"github.com/percona/mongodb-backup/mdbstructs"
)

const (
	baseScore                      = 100
	hiddenMemberMultiplier         = 1.6
	secondaryMemberMultiplier      = 1.1
	priorityZeroMultiplier         = 1.3
	replsetOkLagMultiplier         = 1.1
	minPrioritySecondaryMultiplier = 1.2
	minVotesSecondaryMultiplier    = 1.2
)

var (
	maxOkReplsetLagDuration = 30 * time.Second
	maxReplsetLagDuration   = 5 * time.Minute
)

type ScorerMsg string

const (
	msgMemberDown                 ScorerMsg = "is down"
	msgMemberSecondary            ScorerMsg = "is secondary"
	msgMemberBadState             ScorerMsg = "has bad state"
	msgMemberHidden               ScorerMsg = "is hidden"
	msgMemberPriorityZero         ScorerMsg = "has priority 0"
	msgMemberReplsetLagOk         ScorerMsg = "has ok replset lag"
	msgMemberReplsetLagFail       ScorerMsg = "has high replset lag"
	msgMemberMinPrioritySecondary ScorerMsg = "min priority secondary"
	msgMemberMinVotesSecondary    ScorerMsg = "min votes secondary"
)

type ScoringMember struct {
	config *mdbstructs.ReplsetConfigMember
	status *mdbstructs.ReplsetStatusMember
	score  float64
	log    []ScorerMsg
}

func (sm *ScoringMember) Name() string {
	return sm.config.Host
}

func (sm *ScoringMember) SetScore(score float64, msg ScorerMsg) {
	sm.score = score
	sm.log = append(sm.log, msg)
}

func (sm *ScoringMember) MultiplyScore(multiplier float64, msg ScorerMsg) {
	sm.score *= multiplier
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
		if member.config.Priority == 0 {
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
		if member.config.Votes > 0 && minVotes == nil || member.config.Votes < minVotes.config.Votes {
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

		// secondary only
		if member.status.State == mdbstructs.ReplsetMemberStateSecondary {
			if member.config.Hidden {
				member.MultiplyScore(hiddenMemberMultiplier, msgMemberHidden)
			} else if member.config.Priority == 0 {
				member.MultiplyScore(priorityZeroMultiplier, msgMemberPriorityZero)
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

		fmt.Printf("%v %v %v\n", member.config.Host, member.score, member.log)
	}

	// increase score of secondary with the lowest priority
	minPrioritySecondary := s.minPrioritySecondary()
	if minPrioritySecondary != nil {
		minPrioritySecondary.MultiplyScore(minPrioritySecondaryMultiplier, msgMemberMinPrioritySecondary)
	}

	// increase score of secondary with the lowest number of votes
	minVotesSecondary := s.minVotesSecondary()
	if minVotesSecondary != nil {
		minVotesSecondary.MultiplyScore(minVotesSecondaryMultiplier, msgMemberMinPrioritySecondary)
	}

	//spew.Dump(s.members)

	return nil
}

func (s *Scorer) Winner() *ScoringMember {
	var winner *ScoringMember
	for _, member := range s.members {
		if winner == nil || member.score > winner.score {
			winner = member
		}
	}
	return winner
}
