package main

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/types"
	"github.com/percona/percona-backup-mongodb/internal/util"

	"github.com/percona/percona-backup-mongodb/internal/topo"
	"github.com/percona/percona-backup-mongodb/internal/version"
)

func TestBcpMatchCluster(t *testing.T) {
	type bcase struct {
		meta   types.BackupMeta
		expect defs.Status
	}
	cases := []struct {
		confsrv string
		shards  []topo.Shard
		bcps    []bcase
	}{
		{
			confsrv: "config",
			shards: []topo.Shard{
				{RS: "config"},
				{RS: "rs1"},
				{RS: "rs2"},
			},
			bcps: []bcase{
				{
					types.BackupMeta{
						Name: "bcp1",
						Replsets: []types.BackupReplset{
							{Name: "rs3"},
						},
					},
					defs.StatusError,
				},
				{
					types.BackupMeta{
						Name: "bcp2",
						Replsets: []types.BackupReplset{
							{Name: "config"},
						},
					},
					defs.StatusDone,
				},
				{
					types.BackupMeta{
						Name: "bcp2",
						Replsets: []types.BackupReplset{
							{Name: "rs1"},
							{Name: "rs2"},
						},
					},
					defs.StatusError,
				},
				{
					types.BackupMeta{
						Name: "bcp2",
						Replsets: []types.BackupReplset{
							{Name: "rs1"},
						},
					},
					defs.StatusError,
				},
				{
					types.BackupMeta{
						Name: "bcp3",
						Replsets: []types.BackupReplset{
							{Name: "config"},
							{Name: "rs1"},
							{Name: "rs3"},
						},
					},
					defs.StatusError,
				},
				{
					types.BackupMeta{
						Name: "bcp4",
						Replsets: []types.BackupReplset{
							{Name: "config"},
							{Name: "rs1"},
						},
					},
					defs.StatusDone,
				},
				{
					types.BackupMeta{
						Name: "bcp5",
						Replsets: []types.BackupReplset{
							{Name: "config"},
							{Name: "rs1"},
							{Name: "rs2"},
						},
					},
					defs.StatusDone,
				},
			},
		},
		{
			confsrv: "rs1",
			shards: []topo.Shard{
				{RS: "rs1"},
			},
			bcps: []bcase{
				{
					types.BackupMeta{
						Name: "bcp1",
						Replsets: []types.BackupReplset{
							{Name: "rs3"},
						},
					},
					defs.StatusError,
				},
				{
					types.BackupMeta{
						Name: "bcp2",
						Replsets: []types.BackupReplset{
							{Name: "rs1"},
						},
					},
					defs.StatusDone,
				},
				{
					types.BackupMeta{
						Name: "bcp3",
						Replsets: []types.BackupReplset{
							{Name: "config"},
							{Name: "rs1"},
							{Name: "rs3"},
						},
					},
					defs.StatusError,
				},
				{
					types.BackupMeta{
						Name: "bcp4",
						Replsets: []types.BackupReplset{
							{Name: "config"},
							{Name: "rs1"},
						},
					},
					defs.StatusError,
				},
				{
					types.BackupMeta{
						Name: "bcp5",
						Replsets: []types.BackupReplset{
							{Name: "config"},
							{Name: "rs1"},
							{Name: "rs2"},
						},
					},
					defs.StatusError,
				},
			},
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			m := []types.BackupMeta{}
			for _, b := range c.bcps {
				b.meta.PBMVersion = string(version.Current().Version)
				b.meta.Status = defs.StatusDone
				m = append(m, b.meta)
			}
			bcpsMatchCluster(m, "", "", c.shards, c.confsrv, nil)
			for i := 0; i < len(c.bcps); i++ {
				if c.bcps[i].expect != m[i].Status {
					t.Errorf("wrong status for %s, expect %s, got %s", m[i].Name, c.bcps[i].expect, m[i].Status)
				}
			}
		})
	}
}

func TestBcpMatchRemappedCluster(t *testing.T) {
	defaultTopology := map[string]bool{
		"rs0": true,
		"rs1": false,
	}

	cases := []struct {
		topology map[string]bool
		bcp      types.BackupMeta
		rsMap    map[string]string
		expected error
	}{
		{
			bcp: types.BackupMeta{
				Replsets: []types.BackupReplset{
					{Name: "rs0"},
					{Name: "rs1"},
				},
			},
			rsMap:    map[string]string{},
			expected: nil,
		},
		{
			bcp: types.BackupMeta{
				Replsets: []types.BackupReplset{
					{Name: "rs0"},
				},
			},
			expected: nil,
		},
		{
			bcp: types.BackupMeta{
				Replsets: []types.BackupReplset{
					{Name: "rs0"},
					{Name: "rs1"},
				},
			},
			rsMap: map[string]string{
				"rs0": "rs0",
				"rs1": "rs1",
			},
			expected: nil,
		},
		{
			bcp: types.BackupMeta{
				Replsets: []types.BackupReplset{
					{Name: "rs0"},
					{Name: "rs1"},
					{Name: "rs2"},
				},
			},
			rsMap: map[string]string{},
			expected: missedReplsetsError{
				names: []string{"rs2"},
			},
		},
		{
			bcp: types.BackupMeta{
				Replsets: []types.BackupReplset{
					{Name: "rs0"},
					{Name: "rs1"},
				},
			},
			rsMap: map[string]string{
				"rs1": "rs0",
			},
			expected: missedReplsetsError{
				names: []string{"rs0"},
			},
		},
		{
			topology: map[string]bool{
				"cfg": true,
				"rs0": false,
				"rs1": false,
				"rs2": false,
				"rs3": false,
				"rs4": false,
				"rs6": false,
			},
			bcp: types.BackupMeta{
				Replsets: []types.BackupReplset{
					{Name: "cfg"},
					{Name: "rs0"},
					{Name: "rs1"},
					{Name: "rs2"},
					{Name: "rs3"},
					{Name: "rs4"},
					{Name: "rs5"},
				},
			},
			rsMap: map[string]string{
				"rs0": "rs0",
				"rs1": "rs2",
				"rs2": "rs1",
				"rs4": "rs3",
			},
			expected: missedReplsetsError{
				names: []string{"rs3", "rs5"},
			},
		},
		{
			bcp:      types.BackupMeta{},
			expected: missedReplsetsError{configsrv: true},
		},
	}

	types := []defs.BackupType{
		defs.LogicalBackup,
		defs.PhysicalBackup,
		defs.IncrementalBackup,
	}
	for _, tt := range types {
		t.Logf("backup type: %s", tt)
		for i, c := range cases {
			c.bcp.PBMVersion = string(version.Current().Version)
			topology := defaultTopology
			if c.topology != nil {
				topology = c.topology
			}

			c.bcp.Type = tt
			c.bcp.Status = defs.StatusDone
			mapRS, mapRevRS := util.MakeRSMapFunc(c.rsMap), util.MakeReverseRSMapFunc(c.rsMap)
			bcpMatchCluster(&c.bcp, "", "", topology, mapRS, mapRevRS)

			if msg := checkBcpMatchClusterError(c.bcp.Error(), c.expected); msg != "" {
				t.Errorf("case #%d failed: %s", i, msg)
			} else {
				t.Logf("case #%d ok", i)
			}
		}
	}
}

func checkBcpMatchClusterError(err, target error) string {
	if err == nil && target == nil {
		return ""
	}
	if !errors.Is(err, errIncompatible) {
		return fmt.Sprintf("unknown error: %T", err)
	}

	var err1 missedReplsetsError
	if !errors.As(err, &err1) {
		return fmt.Sprintf("unknown errIncompatible error: %T", err)
	}
	var err2 missedReplsetsError
	if !errors.As(err, &err2) {
		return fmt.Sprintf("expect errMissedReplsets, got %T", err)
	}

	var msg string
	if err1.configsrv != err2.configsrv {
		msg = fmt.Sprintf("expect replsets to be %v, got %v",
			err2.configsrv, err1.configsrv)
	}

	sort.Strings(err1.names)
	sort.Strings(err2.names)

	a := strings.Join(err1.names, ", ")
	b := strings.Join(err2.names, ", ")

	if a != b {
		msg = fmt.Sprintf("expect replsets to be %v, got %v", a, b)
	}

	return msg
}

func BenchmarkBcpMatchCluster3x10(b *testing.B) {
	shards := []topo.Shard{
		{RS: "config"},
		{RS: "rs1"},
		{RS: "rs2"},
	}

	bcps := []types.BackupMeta{}

	for i := 0; i < 10; i++ {
		bcps = append(bcps, types.BackupMeta{
			Name: "bcp",
			Replsets: []types.BackupReplset{
				{Name: "config"},
				{Name: "rs1"},
				{Name: "rs2"},
			},
			Status: defs.StatusDone,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bcpsMatchCluster(bcps, "", "", shards, "config", nil)
	}
}

func BenchmarkBcpMatchCluster3x100(b *testing.B) {
	shards := []topo.Shard{
		{RS: "config"},
		{RS: "rs1"},
		{RS: "rs2"},
	}

	bcps := []types.BackupMeta{}

	for i := 0; i < 100; i++ {
		bcps = append(bcps, types.BackupMeta{
			Name: "bcp",
			Replsets: []types.BackupReplset{
				{Name: "config"},
				{Name: "rs1"},
				{Name: "rs2"},
			},
			Status: defs.StatusDone,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bcpsMatchCluster(bcps, "", "", shards, "config", nil)
	}
}

func BenchmarkBcpMatchCluster17x100(b *testing.B) {
	shards := []topo.Shard{
		{RS: "config"},
		{RS: "rs1"},
		{RS: "rs12"},
		{RS: "rs112"},
		{RS: "rs1112"},
		{RS: "rs11112"},
		{RS: "rs111112"},
		{RS: "rs1111112"},
		{RS: "rs22"},
		{RS: "rs2222"},
		{RS: "rs222222"},
		{RS: "rs222222222"},
		{RS: "rs332"},
		{RS: "rs32"},
		{RS: "rs33332"},
		{RS: "rs333333332"},
		{RS: "rs333333333332"},
	}

	bcps := []types.BackupMeta{}

	for i := 0; i < 100; i++ {
		bcps = append(bcps, types.BackupMeta{
			Name: "bcp3",
			Replsets: []types.BackupReplset{
				{Name: "config"},
				{Name: "rs1"},
				{Name: "rs12"},
				{Name: "rs112"},
				{Name: "rs1112"},
				{Name: "rs11112"},
				{Name: "rs111112"},
				{Name: "rs1111112"},
				{Name: "rs22"},
				{Name: "rs2222"},
				{Name: "rs222222"},
				{Name: "rs222222222"},
				{Name: "rs332"},
				{Name: "rs32"},
				{Name: "rs33332"},
				{Name: "rs333333332"},
				{Name: "rs333333333332"},
			},
			Status: defs.StatusDone,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bcpsMatchCluster(bcps, "", "", shards, "config", nil)
	}
}

func BenchmarkBcpMatchCluster3x1000(b *testing.B) {
	shards := []topo.Shard{
		{RS: "config"},
		{RS: "rs1"},
		{RS: "rs2"},
	}

	bcps := []types.BackupMeta{}

	for i := 0; i < 1000; i++ {
		bcps = append(bcps, types.BackupMeta{
			Name: "bcp3",
			Replsets: []types.BackupReplset{
				{Name: "config"},
				{Name: "rs1"},
				{Name: "rs2"},
			},
			Status: defs.StatusDone,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bcpsMatchCluster(bcps, "", "", shards, "config", nil)
	}
}

func BenchmarkBcpMatchCluster1000x1000(b *testing.B) {
	shards := []topo.Shard{{RS: "config"}}
	rss := []types.BackupReplset{{Name: "config"}}

	for i := 0; i < 1000; i++ {
		shards = append(shards, topo.Shard{RS: fmt.Sprint(i)})
		rss = append(rss, types.BackupReplset{Name: fmt.Sprint(i)})
	}

	bcps := []types.BackupMeta{}

	for i := 0; i < 1000; i++ {
		bcps = append(bcps, types.BackupMeta{
			Replsets: rss,
			Status:   defs.StatusDone,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bcpsMatchCluster(bcps, "", "", shards, "config", nil)
	}
}

func BenchmarkBcpMatchCluster3x10Err(b *testing.B) {
	shards := []topo.Shard{
		{RS: "config"},
		{RS: "rs2"},
	}

	bcps := []types.BackupMeta{
		{
			Name: "bcp1",
			Replsets: []types.BackupReplset{
				{Name: "config"},
				{Name: "rs1"},
				{Name: "rs3"},
			},
			Status: defs.StatusDone,
		},
		{
			Name: "bcp2",
			Replsets: []types.BackupReplset{
				{Name: "config"},
				{Name: "rs1"},
			},
			Status: defs.StatusDone,
		},
	}
	for i := 0; i < 8; i++ {
		bcps = append(bcps, types.BackupMeta{
			Replsets: []types.BackupReplset{
				{Name: "config"},
				{Name: "rs1"},
				{Name: "rs3"},
			},
			Status: defs.StatusDone,
		})
	}
	for i := 0; i < b.N; i++ {
		bcpsMatchCluster(bcps, "", "", shards, "config", nil)
	}
}

func BenchmarkBcpMatchCluster1000x1000Err(b *testing.B) {
	shards := []topo.Shard{{RS: "config"}}
	rss := []types.BackupReplset{{Name: "config"}}

	for i := 0; i < 1000; i++ {
		shards = append(shards, topo.Shard{RS: fmt.Sprint(i)})
		rss = append(rss, types.BackupReplset{Name: fmt.Sprint(i)})
	}

	rss = append(rss, types.BackupReplset{Name: "newrs"})
	bcps := []types.BackupMeta{}

	for i := 0; i < 1000; i++ {
		bcps = append(bcps, types.BackupMeta{
			Replsets: rss,
			Status:   defs.StatusDone,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bcpsMatchCluster(bcps, "", "", shards, "config", nil)
	}
}
