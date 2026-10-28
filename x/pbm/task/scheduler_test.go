package task

import (
	"errors"
	"slices"
	"testing"

	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

func agent(name, rs, me string, mi status.MongoInfo) status.AgentInfo {
	mi.SetName = rs
	mi.Me = me
	return status.AgentInfo{
		Name:        name,
		Alive:       true,
		MongoInfo:   mi,
		AgentStatus: status.SubsysStatus{OK: true},
	}
}

var (
	primary   = status.MongoInfo{IsPrimary: true}
	secondary = status.MongoInfo{Secondary: true}
	hidden    = status.MongoInfo{Secondary: true, Hidden: true}
)

func sharded(mi status.MongoInfo, configSvr bool) status.MongoInfo {
	mi.Sharded = true
	mi.ConfigSvr = configSvr
	return mi
}

func TestResolveForBackup(t *testing.T) {
	t.Run("priority cases", func(t *testing.T) {
		down := agent("rs0-down", "rs0", "h:3", hidden)
		down.Alive = false

		unhealthy := agent("rs0-err", "rs0", "h:4", hidden)
		unhealthy.AgentStatus = status.SubsysStatus{Err: "mongo down"}

		tests := []struct {
			name       string
			members    []status.AgentInfo
			prio       config.Priority
			wantAgents []string
			wantLeader string
		}{
			{
				name: "hidden over secondary over primary",
				members: []status.AgentInfo{
					agent("rs0-p", "rs0", "h:1", primary),
					agent("rs0-s", "rs0", "h:2", secondary),
					agent("rs0-h", "rs0", "h:3", hidden),
				},
				wantAgents: []string{"rs0-h"},
				wantLeader: "rs0-h",
			},
			{
				name: "secondary over primary",
				members: []status.AgentInfo{
					agent("rs0-p", "rs0", "h:1", primary),
					agent("rs0-s", "rs0", "h:2", secondary),
				},
				wantAgents: []string{"rs0-s"},
				wantLeader: "rs0-s",
			},
			{
				name: "primary when it's the only one",
				members: []status.AgentInfo{
					agent("rs0-p", "rs0", "h:1", primary),
					agent("ctrl", "", "", status.MongoInfo{}),
				},
				wantAgents: []string{"rs0-p"},
				wantLeader: "rs0-p",
			},
			{
				// todo: it should be random in this case
				name: "score is the same, prioritize by name",
				members: []status.AgentInfo{
					agent("rs0-b", "rs0", "h:2", secondary),
					agent("rs0-a", "rs0", "h:1", secondary),
				},
				wantAgents: []string{"rs0-a"},
				wantLeader: "rs0-a",
			},
			{
				name: "down and unhealthy agents skipped",
				members: []status.AgentInfo{
					agent("rs0-p", "rs0", "h:1", primary),
					down,
					unhealthy,
				},
				wantAgents: []string{"rs0-p"},
				wantLeader: "rs0-p",
			},
			{
				name: "explicit priorities override implicit ones",
				members: []status.AgentInfo{
					agent("rs0-p", "rs0", "h:1", primary),
					agent("rs0-s", "rs0", "h:2", secondary),
					agent("rs0-h", "rs0", "h:3", hidden),
				},
				prio:       config.Priority{"h:1": 3, "h:3": 0.5},
				wantAgents: []string{"rs0-p"},
				wantLeader: "rs0-p",
			},
			{
				name: "explicit priorities default unlisted members",
				members: []status.AgentInfo{
					agent("rs0-p", "rs0", "h:1", primary),
					agent("rs0-h", "rs0", "h:3", hidden),
				},
				prio:       config.Priority{"h:3": 0.5},
				wantAgents: []string{"rs0-p"},
				wantLeader: "rs0-p",
			},
			{
				name: "sharded cluster led by config server",
				members: []status.AgentInfo{
					agent("rs1-p", "rs1", "a:1", sharded(primary, false)),
					agent("rs1-s", "rs1", "a:2", sharded(secondary, false)),
					agent("cfg-p", "cfg", "c:1", sharded(primary, true)),
					agent("cfg-s", "cfg", "c:2", sharded(secondary, true)),
					agent("rs0-p", "rs0", "b:1", sharded(primary, false)),
				},
				wantAgents: []string{"cfg-s", "rs0-p", "rs1-s"},
				wantLeader: "cfg-s",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				agents, leader, err := resolveForBackup(tc.members, tc.prio)
				if err != nil {
					t.Fatalf("resolveForBackup() error: %v", err)
				}
				if !slices.Equal(agents, tc.wantAgents) {
					t.Errorf("agents = %v, want %v", agents, tc.wantAgents)
				}
				if leader != tc.wantLeader {
					t.Errorf("leader = %q, want %q", leader, tc.wantLeader)
				}
			})
		}
	})

	t.Run("errors cases", func(t *testing.T) {
		down := agent("rs1-p", "rs1", "a:1", primary)
		down.Alive = false

		tests := []struct {
			name    string
			members []status.AgentInfo
		}{
			{
				name: "no members",
			},
			{
				name:    "no mongod attached",
				members: []status.AgentInfo{agent("ctrl", "", "", status.MongoInfo{})},
			},
			{
				name: "replica set with no suitable agent",
				members: []status.AgentInfo{
					agent("rs0-p", "rs0", "h:1", primary),
					down,
				},
			},
			{
				name: "arbiter only",
				members: []status.AgentInfo{
					agent("rs0-a", "rs0", "h:1", status.MongoInfo{ArbiterOnly: true}),
				},
			},
			{
				name: "sharded without config server",
				members: []status.AgentInfo{
					agent("rs0-p", "rs0", "h:1", sharded(primary, false)),
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				_, _, err := resolveForBackup(tc.members, nil)
				if !errors.Is(err, ErrNoAgents) {
					t.Fatalf("resolveForBackup() error = %v, want %v", err, ErrNoAgents)
				}
			})
		}
	})
}
