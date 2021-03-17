package pbm

import (
	"sort"

	"github.com/pkg/errors"
)

const defaultScore = 1.0

// NodesPriority groupes nodes by priority according to
// provided scores. Basically nodes are grouped and sorted by
// descending order by score
type NodesPriority struct {
	m map[string]nscores
}

func NewNodesPriority() *NodesPriority {
	return &NodesPriority{make(map[string]nscores)}
}

// Add node with its score
func (n *NodesPriority) Add(rs, node string, sc float64) {
	s, ok := n.m[rs]
	if !ok {
		s = nscores{m: make(map[float64][]string)}
	}
	s.add(node, sc)
	n.m[rs] = s
}

// RS returns nodes `group and sort desc by score` for given replset
func (n *NodesPriority) RS(rs string) [][]string {
	return n.m[rs].list()
}

type agentScore func(AgentStat) float64

// BcpNodesPriority returns list nodes grouped by backup preferences
// in descended order. First are nodes with the highest priority.
func (p *PBM) BcpNodesPriority() (*NodesPriority, error) {
	cfg, err := p.GetConfig()
	if err != nil {
		return nil, errors.Wrap(err, "get config")
	}
	agnts, err := p.AgentsStatus()
	if err != nil {
		return nil, errors.Wrap(err, "get agents list")
	}

	// by defaul nodes have only two types:
	// - primary - defaultScore
	// - the rest - defaultScore + 1
	//
	// if cfg.Backup.Priority is set - fully rely on config
	// (don't downscore primaries)
	f := func(a AgentStat) float64 {
		if a.State == NodeStatePrimary {
			return defaultScore
		}

		return defaultScore + 1
	}

	if cfg.Backup.Priority != nil || len(cfg.Backup.Priority) > 0 {
		f = func(a AgentStat) float64 {
			sc, ok := cfg.Backup.Priority[a.Node]
			if !ok {
				return defaultScore
			}

			return sc
		}
	}

	return bcpNodesPriority(agnts, f), nil
}

func bcpNodesPriority(agents []AgentStat, f agentScore) *NodesPriority {
	scrs := NewNodesPriority()

	for _, a := range agents {
		if ok, _ := a.OK(); !ok {
			continue
		}

		scrs.Add(a.RS, a.Node, f(a))
	}

	return scrs
}

type nscores struct {
	idx []float64
	m   map[float64][]string
}

func (s *nscores) add(node string, sc float64) {
	nodes, ok := s.m[sc]
	if !ok {
		s.idx = append(s.idx, sc)
	}
	s.m[sc] = append(nodes, node)
}

func (s nscores) list() [][]string {
	ret := make([][]string, len(s.idx))
	sort.Sort(sort.Reverse(sort.Float64Slice(s.idx)))

	for i := range ret {
		ret[i] = s.m[s.idx[i]]
	}

	return ret
}
