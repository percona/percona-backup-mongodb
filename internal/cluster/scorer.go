package cluster

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/percona/mongodb-backup/mdbstructs"
)

const (
	baseScore                      = 100
	hiddenMemberMultiplier         = 1.8
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
	score  int
	log    []ScorerMsg
}

func (sm *ScoringMember) Name() string {
	return sm.config.Host
}

func (sm *ScoringMember) SetScore(score float64, msg ScorerMsg) {
	sm.score = int(math.Floor(score))
	sm.log = append(sm.log, msg)
}

func (sm *ScoringMember) MultiplyScore(multiplier float64, msg ScorerMsg) {
	newScore := float64(sm.score) * multiplier
	sm.score = int(math.Floor(newScore))
	sm.log = append(sm.log, msg)
}

func minPrioritySecondary(members map[string]*ScoringMember) *ScoringMember {
	var minPriority *ScoringMember
	for _, member := range members {
		if member.status.State != mdbstructs.ReplsetMemberStateSecondary || member.config.Priority < 1 {
			continue
		}
		if minPriority == nil || member.config.Priority < minPriority.config.Priority {
			minPriority = member
		}
	}
	return minPriority
}

func minVotesSecondary(members map[string]*ScoringMember) *ScoringMember {
	var minVotes *ScoringMember
	for _, member := range members {
		if member.status.State != mdbstructs.ReplsetMemberStateSecondary || member.config.Votes < 1 {
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

type Scorer struct {
	status  *mdbstructs.ReplsetStatus
	tags    *mdbstructs.ReplsetTags
	members map[string]*ScoringMember
}

func NewScorer() *Scorer {
	return &Scorer{
		members: map[string]*ScoringMember{},
	}
}

func ScoreReplset(config *mdbstructs.ReplsetConfig, status *mdbstructs.ReplsetStatus, tags *mdbstructs.ReplsetTags) (*Scorer, error) {
	var err error
	var secondariesWithPriority int
	var secondariesWithVotes int

	scorer := &Scorer{
		status: status,
		tags:   tags,
	}
	scorer.members, err = getScoringMembers(config, status)
	if err != nil {
		return nil, err
	}

	for _, member := range scorer.members {
		// replset health
		if member.status.Health != mdbstructs.ReplsetMemberHealthUp {
			member.SetScore(0, msgMemberDown)
			continue
		}

		// replset state
		if member.status.State == mdbstructs.ReplsetMemberStateSecondary {
			member.MultiplyScore(secondaryMemberMultiplier, msgMemberSecondary)
		} else if member.status.State != mdbstructs.ReplsetMemberStatePrimary {
			member.SetScore(0, msgMemberBadState)
			continue
		}

		// secondaries only
		if member.status.State == mdbstructs.ReplsetMemberStateSecondary {
			if member.config.Hidden {
				member.MultiplyScore(hiddenMemberMultiplier, msgMemberHidden)
			} else if member.config.Priority == 0 {
				member.MultiplyScore(priorityZeroMultiplier, msgMemberPriorityZero)
			} else {
				secondariesWithPriority++
				if member.config.Votes > 0 {
					secondariesWithVotes++
				}
			}

			replsetLag, err := GetReplsetLagDuration(status, GetReplsetStatusMember(status, member.config.Host))
			if err != nil {
				return nil, err
			}
			if replsetLag < maxOkReplsetLagDuration {
				member.MultiplyScore(replsetOkLagMultiplier, msgMemberReplsetLagOk)
			} else if replsetLag >= maxReplsetLagDuration {
				member.SetScore(0, msgMemberReplsetLagFail)
			}
		}
	}

	// if there is more than one secondary with priority > 1, increase
	// score of secondary with the lowest priority
	if secondariesWithPriority > 1 {
		minPrioritySecondary := minPrioritySecondary(scorer.members)
		if minPrioritySecondary != nil {
			minPrioritySecondary.MultiplyScore(minPrioritySecondaryMultiplier, msgMemberMinPrioritySecondary)
		}
	}

	// if there is more than one secondary with votes > 1, increase
	// score of secondary with the lowest number of votes
	if secondariesWithVotes > 1 {
		fmt.Println(secondariesWithVotes)
		minVotesSecondary := minVotesSecondary(scorer.members)
		if minVotesSecondary != nil {
			minVotesSecondary.MultiplyScore(minVotesSecondaryMultiplier, msgMemberMinVotesSecondary)
		}
	}

	return scorer, nil
}

func (s *Scorer) Winner() *ScoringMember {
	var winner *ScoringMember
	for _, member := range s.members {
		if member.score > 0 && winner == nil || member.score > winner.score {
			winner = member
		}
	}
	return winner
}
