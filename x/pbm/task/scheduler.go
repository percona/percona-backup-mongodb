package task

import (
	"cmp"
	"context"
	"slices"

	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

// ErrNoAgents is returned when the scheduler cannot find a suitable agent.
var ErrNoAgents = errors.New("no suitable agents")

// Implicit priority scores, applied when the config sets no explicit priorities.
// The higher the score, the more preferred the agent is.
const (
	defaultScore   = 1.0
	scorePrimary   = defaultScore / 2
	scoreSecondary = defaultScore
	scoreHidden    = defaultScore * 2
)

// Scheduler resolves which agents take part in a cluster-wide task.
type Scheduler struct {
	statusSvc *status.Svc
	configSvc *config.Svc
}

// NewScheduler creates a scheduler.
func NewScheduler(statusSvc *status.Svc, configSvc *config.Svc) *Scheduler {
	return &Scheduler{statusSvc: statusSvc, configSvc: configSvc}
}

// ResolveAgentsForBackup picks one agent per replica set to run the backup.
// It also returns backup leader as the second parameter.
func (s *Scheduler) ResolveAgentsForBackup(ctx context.Context) ([]string, string, error) {
	prio, err := s.backupPriority(ctx)
	if err != nil {
		return nil, "", err
	}

	return resolveForBackup(s.statusSvc.GetAllMembers(), prio)
}

// backupPriority returns explicit backup priorities from the main config.
// It returns empty priorities when there is no config or it sets none.
func (s *Scheduler) backupPriority(ctx context.Context) (config.Priority, error) {
	cfg, err := s.configSvc.Get(ctx, config.DefaultConfigName)
	if err != nil {
		if errors.Is(err, config.ErrNotFound) {
			return config.Priority{}, nil
		}
		return nil, errors.Wrap(err, "get config")
	}
	if cfg.Backup == nil {
		return config.Priority{}, nil
	}

	return cfg.Backup.Priority, nil
}

// resolveForBackup picks the best agent of every replica set known to members.
// A replica set with no suitable agent fails the whole resolution.
func resolveForBackup(members []status.AgentInfo, prio config.Priority) ([]string, string, error) {
	byRS := make(map[string][]status.AgentInfo)
	for _, m := range members {
		rs := m.MongoInfo.SetName
		if rs == "" {
			// agent with no mongod attached
			continue
		}
		if _, ok := byRS[rs]; !ok {
			// keep the replica set known even if none of its agents suits
			byRS[rs] = nil
		}
		if isSuitable(m) {
			byRS[rs] = append(byRS[rs], m)
		}
	}
	if len(byRS) == 0 {
		return nil, "", errors.Wrap(ErrNoAgents, "no replica sets found")
	}

	rsNames := make([]string, 0, len(byRS))
	for rs := range byRS {
		rsNames = append(rsNames, rs)
	}
	slices.Sort(rsNames)

	agents := make([]string, 0, len(rsNames))
	var leader string
	for _, rs := range rsNames {
		best, ok := pickBest(byRS[rs], prio)
		if !ok {
			return nil, "", errors.Wrapf(ErrNoAgents, "replica set %q", rs)
		}
		agents = append(agents, best.Name)

		if best.MongoInfo.ConfigSvr || (leader == "" && !best.MongoInfo.Sharded) {
			leader = best.Name
		}
	}
	if leader == "" {
		return nil, "", errors.Wrap(ErrNoAgents, "no leader: config server replica set not found")
	}

	return agents, leader, nil
}

// isSuitable reports whether the agent is able to run the backup.
func isSuitable(m status.AgentInfo) bool {
	mi := m.MongoInfo
	return m.Alive && m.AgentStatus.OK && !mi.ArbiterOnly && (mi.IsPrimary || mi.Secondary)
}

// pickBest returns the agent with the highest score.
func pickBest(candidates []status.AgentInfo, prio config.Priority) (status.AgentInfo, bool) {
	if len(candidates) == 0 {
		return status.AgentInfo{}, false
	}

	best := slices.MaxFunc(candidates, func(a, b status.AgentInfo) int {
		if c := cmp.Compare(score(a, prio), score(b, prio)); c != 0 {
			return c
		}
		// MaxFunc prefers the greater one, so the lower name must compare greater.
		return cmp.Compare(b.Name, a.Name)
	})
	return best, true
}

// score calculates the agent's priority.
func score(m status.AgentInfo, prio config.Priority) float64 {
	if len(prio) > 0 {
		sc, ok := prio[m.MongoInfo.Me]
		if !ok || sc < 0 {
			return defaultScore
		}
		return sc
	}

	switch {
	case m.MongoInfo.IsPrimary:
		return scorePrimary
	case m.MongoInfo.Hidden:
		return scoreHidden
	default:
		return scoreSecondary
	}
}
