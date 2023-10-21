package main

import (
	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/topo"
	"reflect"
	"testing"
)

func Test_customPriorityCalculator(t *testing.T) {
	cfg := &config.Config{
		Backup: config.BackupConf{
			Priority: map[string]float64{
				"node1":              2.5,
				"tag:someTag=value1": 3,
				"node4":              4,
				"tag:someTag=value4": 3,
			},
		},
	}

	tests := []struct {
		name      string
		agentStat topo.AgentStat
		want      float64
	}{
		{
			name:      "node selector, non existing node",
			agentStat: topo.AgentStat{Node: "node0"},
			want:      1,
		},
		{
			name:      "node selector, existing node",
			agentStat: topo.AgentStat{Node: "node1"},
			want:      2.5,
		},
		{
			name: "tag selector, non existing tag",
			agentStat: topo.AgentStat{
				Node: "node2",
				Tags: map[string]string{"someTag": "value0"},
			},
			want: 1.0,
		},
		{
			name: "tag selector, existing tag",
			agentStat: topo.AgentStat{
				Node: "node2",
				Tags: map[string]string{"someTag": "value1"},
			},
			want: 3,
		},
		{
			name: "node < tag",
			agentStat: topo.AgentStat{
				Node: "node1",
				Tags: map[string]string{"someTag": "value1"},
			},
			want: 3,
		},
		{
			name: "node > tag",
			agentStat: topo.AgentStat{
				Node: "node4",
				Tags: map[string]string{"someTag": "value4"},
			},
			want: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := customPriorityCalculator(cfg)(tt.agentStat); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("customPriorityCalculator() = %v, want %v", got, tt.want)
			}
		})
	}
}
