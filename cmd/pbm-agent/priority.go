package main

import (
	"context"
	"fmt"
	"sort"

	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/topo"
)

const (
	defaultScore = 1.0
	tagPrefix    = "tag"
)

// NodesPriority groups nodes by priority according to
// provided scores. Basically nodes are grouped and sorted by
// descending order by score
type NodesPriority struct {
	m map[string]nodeScores
}

func NewNodesPriority() *NodesPriority {
	return &NodesPriority{make(map[string]nodeScores)}
}

// Add node with its score
func (n *NodesPriority) Add(rs, node string, sc float64) {
	s, ok := n.m[rs]
	if !ok {
		s = nodeScores{m: make(map[float64][]string)}
	}
	s.add(node, sc)
	n.m[rs] = s
}

// RS returns nodes `group and sort desc by score` for given replset
func (n *NodesPriority) RS(rs string) [][]string {
	return n.m[rs].list()
}

type agentScore func(topo.AgentStat) float64

// BcpNodesPriority returns list nodes grouped by backup preferences
// in descended order. First are nodes with the highest priority.
// Custom coefficients might be passed. These will be ignored though
// if the config is set.
func BcpNodesPriority(
	ctx context.Context,
	m connect.Client,
	c map[string]float64,
	agents []topo.AgentStat,
) (*NodesPriority, error) {
	cfg, err := config.GetConfig(ctx, m)
	if err != nil {
		return nil, errors.Wrap(err, "get config")
	}

	// if cfg.Backup.Priority doesn't set apply defaults
	f := func(a topo.AgentStat) float64 {
		if coeff, ok := c[a.Node]; ok && c != nil {
			return defaultScore * coeff
		} else if a.State == defs.NodeStatePrimary {
			return defaultScore / 2
		} else if a.Hidden {
			return defaultScore * 2
		}
		return defaultScore
	}

	if cfg.Backup.Priority != nil || len(cfg.Backup.Priority) > 0 {
		f = customPriorityCalculator(cfg)
	}

	return bcpNodesPriority(agents, f), nil
}

func customPriorityCalculator(cfg *config.Config) func(a topo.AgentStat) float64 {
	return func(a topo.AgentStat) float64 {
		sc, ok := cfg.Backup.Priority[a.Node]
		if !ok {
			sc = -1
		}

		for k, v := range a.Tags {
			tagSc, ok := (cfg.Backup.Priority[fmt.Sprintf("%s:%s=%s", tagPrefix, k, v)])
			if ok && tagSc > sc {
				sc = tagSc
			}
		}
		if sc > 0 {
			return sc
		} else {
			return defaultScore
		}
	}
}

func bcpNodesPriority(agents []topo.AgentStat, f agentScore) *NodesPriority {
	scores := NewNodesPriority()

	for _, a := range agents {
		if ok, _ := a.OK(); !ok {
			continue
		}

		scores.Add(a.RS, a.Node, f(a))
	}

	return scores
}

type nodeScores struct {
	idx []float64
	m   map[float64][]string
}

func (s *nodeScores) add(node string, sc float64) {
	nodes, ok := s.m[sc]
	if !ok {
		s.idx = append(s.idx, sc)
	}
	s.m[sc] = append(nodes, node)
}

func (s nodeScores) list() [][]string {
	ret := make([][]string, len(s.idx))
	sort.Sort(sort.Reverse(sort.Float64Slice(s.idx)))

	for i := range ret {
		ret[i] = s.m[s.idx[i]]
	}

	return ret
}
