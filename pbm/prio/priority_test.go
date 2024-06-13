package prio

import (
	"context"
	"reflect"
	"testing"

	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
)

func TestCalcNodesPriority(t *testing.T) {
	t.Run("implicit priorities - rs", func(t *testing.T) {
		testCases := []struct {
			desc   string
			agents []topo.AgentStat
			res    [][]string
		}{
			{
				desc: "implicit priorities for PSS",
				agents: []topo.AgentStat{
					newP("rs0", "rs01"),
					newS("rs0", "rs02"),
					newS("rs0", "rs03"),
				},
				res: [][]string{
					{"rs02", "rs03"},
					{"rs01"},
				},
			},
			{
				desc: "implicit priorities for PSH",
				agents: []topo.AgentStat{
					newP("rs0", "rs01"),
					newS("rs0", "rs02"),
					newH("rs0", "rs03"),
				},
				res: [][]string{
					{"rs03"},
					{"rs02"},
					{"rs01"},
				},
			},
			{
				desc: "implicit priorities for PSA",
				agents: []topo.AgentStat{
					newP("rs0", "rs01"),
					newS("rs0", "rs02"),
					newA("rs0", "rs03"),
				},
				res: [][]string{
					{"rs02"},
					{"rs01"},
				},
			},
			{
				desc: "5 members mix",
				agents: []topo.AgentStat{
					newS("rs0", "rs01"),
					newH("rs0", "rs02"),
					newP("rs0", "rs03"),
					newA("rs0", "rs04"),
					newH("rs0", "rs05"),
				},
				res: [][]string{
					{"rs02", "rs05"},
					{"rs01"},
					{"rs03"},
				},
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				np, err := CalcNodesPriority(context.Background(), nil, nil, tC.agents)
				if err != nil {
					t.Fatalf("unexpected error while calculating nodes priority: %v", err)
				}

				prioByScore := np.RS(tC.agents[0].RS)

				if !reflect.DeepEqual(prioByScore, tC.res) {
					t.Fatalf("wrong nodes priority calculation: want=%v, got=%v", tC.res, prioByScore)
				}
			})
		}
	})

	t.Run("implicit priorities - sharded cluster", func(t *testing.T) {
		testCases := []struct {
			desc   string
			agents []topo.AgentStat
			resCfg [][]string
			resRS0 [][]string
			resRS1 [][]string
		}{
			{
				desc: "implicit priorities for PSS",
				agents: []topo.AgentStat{
					newP("rs0", "rs01"),
					newS("rs0", "rs02"),
					newS("rs0", "rs03"),
					newS("rs1", "rs11"),
					newP("rs1", "rs12"),
					newS("rs1", "rs13"),
					newS("cfg", "cfg1"),
					newS("cfg", "cfg2"),
					newP("cfg", "cfg3"),
				},
				resCfg: [][]string{
					{"cfg1", "cfg2"},
					{"cfg3"},
				},
				resRS0: [][]string{
					{"rs02", "rs03"},
					{"rs01"},
				},
				resRS1: [][]string{
					{"rs11", "rs13"},
					{"rs12"},
				},
			},
			{
				desc: "implicit priorities for sharded mix",
				agents: []topo.AgentStat{
					newS("cfg", "cfg1"),
					newP("cfg", "cfg2"),
					newS("cfg", "cfg3"),
					newS("rs0", "rs01"),
					newP("rs0", "rs02"),
					newA("rs0", "rs03"),
					newP("rs1", "rs11"),
					newH("rs1", "rs12"),
					newS("rs1", "rs13"),
				},
				resCfg: [][]string{
					{"cfg1", "cfg3"},
					{"cfg2"},
				},
				resRS0: [][]string{
					{"rs01"},
					{"rs02"},
				},
				resRS1: [][]string{
					{"rs12"},
					{"rs13"},
					{"rs11"},
				},
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				np, err := CalcNodesPriority(context.Background(), nil, nil, tC.agents)
				if err != nil {
					t.Fatalf("unexpected error while calculating nodes priority: %v", err)
				}

				prioByScoreCfg := np.RS("cfg")
				prioByScoreRs0 := np.RS("rs0")
				prioByScoreRs1 := np.RS("rs1")

				if !reflect.DeepEqual(prioByScoreCfg, tC.resCfg) {
					t.Fatalf("wrong nodes priority calculation for config cluster: want=%v, got=%v", tC.resCfg, prioByScoreCfg)
				}
				if !reflect.DeepEqual(prioByScoreRs0, tC.resRS0) {
					t.Fatalf("wrong nodes priority calculation for rs1 cluster: want=%v, got=%v", tC.resRS0, prioByScoreRs0)
				}
				if !reflect.DeepEqual(prioByScoreRs1, tC.resRS1) {
					t.Fatalf("wrong nodes priority calculation for rs2 cluster: want=%v, got=%v", tC.resRS1, prioByScoreRs1)
				}
			})
		}
	})

	t.Run("explicit priorities - rs", func(t *testing.T) {
		testCases := []struct {
			desc    string
			agents  []topo.AgentStat
			expPrio map[string]float64
			res     [][]string
		}{
			{
				desc: "all priorities are different",
				agents: []topo.AgentStat{
					newP("rs0", "rs01"),
					newS("rs0", "rs02"),
					newS("rs0", "rs03"),
				},
				expPrio: map[string]float64{
					"rs01": 2.0,
					"rs02": 3.0,
					"rs03": 1.0,
				},
				res: [][]string{
					{"rs02"},
					{"rs01"},
					{"rs03"},
				},
			},
			{
				desc: "5 members, 3 different priority groups",
				agents: []topo.AgentStat{
					newP("rs0", "rs01"),
					newS("rs0", "rs02"),
					newS("rs0", "rs03"),
					newS("rs0", "rs04"),
					newS("rs0", "rs05"),
				},
				expPrio: map[string]float64{
					"rs01": 2.0,
					"rs02": 3.0,
					"rs03": 1.0,
					"rs04": 1.0,
					"rs05": 3.0,
				},
				res: [][]string{
					{"rs02", "rs05"},
					{"rs01"},
					{"rs03", "rs04"},
				},
			},
			{
				desc: "default priorities",
				agents: []topo.AgentStat{
					newP("rs0", "rs01"),
					newS("rs0", "rs02"),
					newS("rs0", "rs03"),
				},
				expPrio: map[string]float64{
					"rs01": 0.5,
				},
				res: [][]string{
					{"rs02", "rs03"},
					{"rs01"},
				},
			},
			{
				desc: "all defaults",
				agents: []topo.AgentStat{
					newP("rs0", "rs01"),
					newS("rs0", "rs02"),
					newS("rs0", "rs03"),
				},
				expPrio: map[string]float64{},
				res: [][]string{
					{"rs01", "rs02", "rs03"},
				},
			},
			{
				desc: "priorities are not defined -> implicit are applied",
				agents: []topo.AgentStat{
					newP("rs0", "rs01"),
					newS("rs0", "rs02"),
					newS("rs0", "rs03"),
				},
				expPrio: nil,
				res: [][]string{
					{"rs02", "rs03"},
					{"rs01"},
				},
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				np, err := CalcNodesPriority(context.Background(), nil, tC.expPrio, tC.agents)
				if err != nil {
					t.Fatalf("unexpected error while calculating nodes priority: %v", err)
				}

				prioByScore := np.RS(tC.agents[0].RS)

				if !reflect.DeepEqual(prioByScore, tC.res) {
					t.Fatalf("wrong nodes priority calculation: want=%v, got=%v", tC.res, prioByScore)
				}
			})
		}
	})

	t.Run("explicit priorities - sharded cluster", func(t *testing.T) {
		testCases := []struct {
			desc    string
			agents  []topo.AgentStat
			expPrio map[string]float64
			res     [][]string
			resCfg  [][]string
			resRS0  [][]string
			resRS1  [][]string
		}{
			{
				desc: "all priorities are different",
				agents: []topo.AgentStat{
					newP("rs0", "rs01"),
					newS("rs0", "rs02"),
					newS("rs0", "rs03"),
					newS("rs1", "rs11"),
					newP("rs1", "rs12"),
					newS("rs1", "rs13"),
					newS("cfg", "cfg1"),
					newS("cfg", "cfg2"),
					newP("cfg", "cfg3"),
				},
				expPrio: map[string]float64{
					"rs01": 2.0,
					"rs02": 3.0,
					"rs03": 1.0,
					"rs11": 2.0,
					"rs12": 3.0,
					"rs13": 1.0,
					"cfg2": 2.0,
				},
				resCfg: [][]string{
					{"cfg2"},
					{"cfg1", "cfg3"},
				},
				resRS0: [][]string{
					{"rs02"},
					{"rs01"},
					{"rs03"},
				},
				resRS1: [][]string{
					{"rs12"},
					{"rs11"},
					{"rs13"},
				},
			},
			{
				desc: "only primary is down prioritized",
				agents: []topo.AgentStat{
					newP("rs0", "rs01"),
					newS("rs0", "rs02"),
					newS("rs0", "rs03"),
					newS("rs1", "rs11"),
					newP("rs1", "rs12"),
					newS("rs1", "rs13"),
					newS("cfg", "cfg1"),
					newS("cfg", "cfg2"),
					newP("cfg", "cfg3"),
				},
				expPrio: map[string]float64{
					"rs01": 0.5,
					"rs12": 0.5,
					"cfg3": 0.5,
				},
				resCfg: [][]string{
					{"cfg1", "cfg2"},
					{"cfg3"},
				},
				resRS0: [][]string{
					{"rs02", "rs03"},
					{"rs01"},
				},
				resRS1: [][]string{
					{"rs11", "rs13"},
					{"rs12"},
				},
			},
		}
		for _, tC := range testCases {
			t.Run(tC.desc, func(t *testing.T) {
				np, err := CalcNodesPriority(context.Background(), nil, tC.expPrio, tC.agents)
				if err != nil {
					t.Fatalf("unexpected error while calculating nodes priority: %v", err)
				}

				prioByScoreCfg := np.RS("cfg")
				prioByScoreRs0 := np.RS("rs0")
				prioByScoreRs1 := np.RS("rs1")

				if !reflect.DeepEqual(prioByScoreCfg, tC.resCfg) {
					t.Fatalf("wrong nodes priority calculation for config cluster: want=%v, got=%v", tC.resCfg, prioByScoreCfg)
				}
				if !reflect.DeepEqual(prioByScoreRs0, tC.resRS0) {
					t.Fatalf("wrong nodes priority calculation for rs1 cluster: want=%v, got=%v", tC.resRS0, prioByScoreRs0)
				}
				if !reflect.DeepEqual(prioByScoreRs1, tC.resRS1) {
					t.Fatalf("wrong nodes priority calculation for rs2 cluster: want=%v, got=%v", tC.resRS1, prioByScoreRs1)
				}
			})
		}
	})

	t.Run("coeficients", func(t *testing.T) {
		agents := []topo.AgentStat{
			newP("rs0", "rs01"),
			newS("rs0", "rs02"),
			newS("rs0", "rs03"),
		}
		res := [][]string{
			{"rs03"},
			{"rs02"},
			{"rs01"},
		}
		c := map[string]float64{
			"rs03": 3.0,
		}

		np, err := CalcNodesPriority(context.Background(), c, nil, agents)
		if err != nil {
			t.Fatalf("unexpected error while calculating nodes priority: %v", err)
		}

		prioByScore := np.RS(agents[0].RS)
		if !reflect.DeepEqual(prioByScore, res) {
			t.Fatalf("wrong nodes priority calculation: want=%v, got=%v", res, prioByScore)
		}
	})
}

func newP(rs, node string) topo.AgentStat {
	return newAgent(rs, node, defs.NodeStatePrimary, false)
}

func newS(rs, node string) topo.AgentStat {
	return newAgent(rs, node, defs.NodeStateSecondary, false)
}

func newH(rs, node string) topo.AgentStat {
	return newAgent(rs, node, defs.NodeStateSecondary, true)
}

func newA(rs, node string) topo.AgentStat {
	return newAgent(rs, node, defs.NodeStateArbiter, false)
}

func newAgent(rs, node string, state defs.NodeState, isHidden bool) topo.AgentStat {
	return topo.AgentStat{
		Node:    node,
		RS:      rs,
		State:   state,
		Hidden:  isHidden,
		Arbiter: state == defs.NodeStateArbiter,
		PBMStatus: topo.SubsysStatus{
			OK: true,
		},
		NodeStatus: topo.SubsysStatus{
			OK: state == defs.NodeStatePrimary || state == defs.NodeStateSecondary,
		},
		StorageStatus: topo.SubsysStatus{
			OK: true,
		},
	}
}
