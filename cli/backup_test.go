package cli

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func TestBcpMatchCluster(t *testing.T) {
	type bcase struct {
		meta   pbm.BackupMeta
		expect pbm.Status
	}
	cases := []struct {
		confsrv string
		shards  []pbm.Shard
		bcps    []bcase
	}{
		{
			confsrv: "config",
			shards: []pbm.Shard{
				{RS: "config"},
				{RS: "rs1"},
				{RS: "rs2"},
			},
			bcps: []bcase{
				{
					pbm.BackupMeta{
						Name: "bcp1",
						Replsets: []pbm.BackupReplset{
							{Name: "rs3"},
						},
					},
					pbm.StatusError,
				},
				{
					pbm.BackupMeta{
						Name: "bcp2",
						Replsets: []pbm.BackupReplset{
							{Name: "config"},
						},
					},
					pbm.StatusDone,
				},
				{
					pbm.BackupMeta{
						Name: "bcp2",
						Replsets: []pbm.BackupReplset{
							{Name: "rs1"},
							{Name: "rs2"},
						},
					},
					pbm.StatusError,
				},
				{
					pbm.BackupMeta{
						Name: "bcp2",
						Replsets: []pbm.BackupReplset{
							{Name: "rs1"},
						},
					},
					pbm.StatusError,
				},
				{
					pbm.BackupMeta{
						Name: "bcp3",
						Replsets: []pbm.BackupReplset{
							{Name: "config"},
							{Name: "rs1"},
							{Name: "rs3"},
						},
					},
					pbm.StatusError,
				},
				{
					pbm.BackupMeta{
						Name: "bcp4",
						Replsets: []pbm.BackupReplset{
							{Name: "config"},
							{Name: "rs1"},
						},
					},
					pbm.StatusDone,
				},
				{
					pbm.BackupMeta{
						Name: "bcp5",
						Replsets: []pbm.BackupReplset{
							{Name: "config"},
							{Name: "rs1"},
							{Name: "rs2"},
						},
					},
					pbm.StatusDone,
				},
			},
		},
		{
			confsrv: "rs1",
			shards: []pbm.Shard{
				{RS: "rs1"},
			},
			bcps: []bcase{
				{
					pbm.BackupMeta{
						Name: "bcp1",
						Replsets: []pbm.BackupReplset{
							{Name: "rs3"},
						},
					},
					pbm.StatusError,
				},
				{
					pbm.BackupMeta{
						Name: "bcp2",
						Replsets: []pbm.BackupReplset{
							{Name: "rs1"},
						},
					},
					pbm.StatusDone,
				},
				{
					pbm.BackupMeta{
						Name: "bcp3",
						Replsets: []pbm.BackupReplset{
							{Name: "config"},
							{Name: "rs1"},
							{Name: "rs3"},
						},
					},
					pbm.StatusError,
				},
				{
					pbm.BackupMeta{
						Name: "bcp4",
						Replsets: []pbm.BackupReplset{
							{Name: "config"},
							{Name: "rs1"},
						},
					},
					pbm.StatusError,
				},
				{
					pbm.BackupMeta{
						Name: "bcp5",
						Replsets: []pbm.BackupReplset{
							{Name: "config"},
							{Name: "rs1"},
							{Name: "rs2"},
						},
					},
					pbm.StatusError,
				},
			},
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			m := []pbm.BackupMeta{}
			for _, b := range c.bcps {
				b.meta.Status = pbm.StatusDone
				m = append(m, b.meta)
			}
			bcpsMatchCluster(m, c.shards, c.confsrv, nil)
			for i := 0; i < len(c.bcps); i++ {
				if c.bcps[i].expect != m[i].Status {
					t.Errorf("wrong status for %s, expect %s, got %s", m[i].Name, c.bcps[i].expect, m[i].Status)
				}
			}
		})
	}
}

func TestBcpMatchRemappedCluster(t *testing.T) {
	var buf []string
	defaultTopology := map[string]struct{}{
		"rs0": {},
		"rs1": {},
	}

	cases := []struct {
		configsrv string
		topology  map[string]struct{}
		bcp       pbm.BackupMeta
		rsMap     map[string]string
		expected  error
	}{
		{
			bcp: pbm.BackupMeta{
				Replsets: []pbm.BackupReplset{
					{Name: "rs0"},
					{Name: "rs1"},
				},
			},
			rsMap:    map[string]string{},
			expected: nil,
		},
		{
			bcp: pbm.BackupMeta{
				Replsets: []pbm.BackupReplset{
					{Name: "rs0"},
				},
			},
			expected: nil,
		},
		{
			bcp: pbm.BackupMeta{
				Replsets: []pbm.BackupReplset{
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
			bcp: pbm.BackupMeta{
				Replsets: []pbm.BackupReplset{
					{Name: "rs0"},
					{Name: "rs1"},
					{Name: "rs2"},
				},
			},
			rsMap: map[string]string{},
			expected: errMissedReplsets{
				names: []string{"rs2"},
			},
		},
		// {
		// 	bcp: pbm.BackupMeta{
		// 		Replsets: []pbm.BackupReplset{
		// 			{Name: "rs0"},
		// 			{Name: "rs1"},
		// 		},
		// 	},
		// 	rsMap: map[string]string{
		// 		"rs1": "rs0",
		// 	},
		// 	expected: errMissedReplsets{
		// 		replsets: []string{"rs1"},
		// 	},
		// },
		{
			configsrv: "cfg",
			topology: map[string]struct{}{
				"cfg": {},
				"rs0": {},
				"rs1": {},
				"rs2": {},
				"rs3": {},
				"rs4": {},
				"rs6": {},
			},
			bcp: pbm.BackupMeta{
				Replsets: []pbm.BackupReplset{
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
			expected: errMissedReplsets{
				names: []string{
					// "rs3",
					"rs5",
				},
			},
		},
		{
			configsrv: "cfg",
			bcp:       pbm.BackupMeta{},
			expected:  errMissedReplsets{configsrv: true},
		},
		{
			bcp: pbm.BackupMeta{
				Type: pbm.PhysicalBackup,
			},
			rsMap:    map[string]string{"": ""},
			expected: errRSMappingWithPhysBackup{},
		},
	}

	for i, c := range cases {
		configsrv := "rs0"
		if c.configsrv != "" {
			configsrv = c.configsrv
		}
		topology := defaultTopology
		if c.topology != nil {
			topology = c.topology
		}

		c.bcp.Status = pbm.StatusDone
		bcpMatchCluster(&c.bcp, topology, configsrv, buf, c.rsMap)

		if msg := checkBcpMatchClusterError(c.bcp.Error(), c.expected); msg != "" {
			t.Errorf("case #%d failed: %s", i, msg)
		} else {
			t.Logf("case #%d ok", i)
		}
	}
}

func checkBcpMatchClusterError(err error, target error) string {
	if errors.Is(err, target) {
		return ""
	}

	if !errors.Is(err, errIncompatible) {
		return fmt.Sprintf("unknown error: %T", err)
	}

	switch err := err.(type) {
	case errMissedReplsets:
		target, ok := target.(errMissedReplsets)
		if !ok {
			return fmt.Sprintf("expect errMissedReplsets, got %T", err)
		}

		var msg string

		if err.configsrv != target.configsrv {
			msg = fmt.Sprintf("expect replsets to be %v, got %v",
				target.configsrv, err.configsrv)
		}

		sort.Strings(err.names)
		sort.Strings(target.names)

		a := strings.Join(err.names, ", ")
		b := strings.Join(target.names, ", ")

		if a != b {
			msg = fmt.Sprintf("expect replsets to be %v, got %v", a, b)
		}

		return msg
	case errRSMappingWithPhysBackup:
		if _, ok := target.(errRSMappingWithPhysBackup); !ok {
			return fmt.Sprintf("expect errRSMappingWithPhysBackup, got %T", err)
		}
	default:
		return fmt.Sprintf("unknown errIncompatible error: %T", err)
	}

	return ""
}

func BenchmarkBcpMatchCluster3x10(b *testing.B) {
	shards := []pbm.Shard{
		{RS: "config"},
		{RS: "rs1"},
		{RS: "rs2"},
	}

	bcps := []pbm.BackupMeta{}

	for i := 0; i < 10; i++ {
		bcps = append(bcps, pbm.BackupMeta{
			Name: "bcp",
			Replsets: []pbm.BackupReplset{
				{Name: "config"},
				{Name: "rs1"},
				{Name: "rs2"},
			},
			Status: pbm.StatusDone,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bcpsMatchCluster(bcps, shards, "config", nil)
	}
}

func BenchmarkBcpMatchCluster3x100(b *testing.B) {
	shards := []pbm.Shard{
		{RS: "config"},
		{RS: "rs1"},
		{RS: "rs2"},
	}

	bcps := []pbm.BackupMeta{}

	for i := 0; i < 100; i++ {
		bcps = append(bcps, pbm.BackupMeta{
			Name: "bcp",
			Replsets: []pbm.BackupReplset{
				{Name: "config"},
				{Name: "rs1"},
				{Name: "rs2"},
			},
			Status: pbm.StatusDone,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bcpsMatchCluster(bcps, shards, "config", nil)
	}
}

func BenchmarkBcpMatchCluster17x100(b *testing.B) {
	shards := []pbm.Shard{
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

	bcps := []pbm.BackupMeta{}

	for i := 0; i < 100; i++ {
		bcps = append(bcps, pbm.BackupMeta{
			Name: "bcp3",
			Replsets: []pbm.BackupReplset{
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
			Status: pbm.StatusDone,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bcpsMatchCluster(bcps, shards, "config", nil)
	}
}

func BenchmarkBcpMatchCluster3x1000(b *testing.B) {
	shards := []pbm.Shard{
		{RS: "config"},
		{RS: "rs1"},
		{RS: "rs2"},
	}

	bcps := []pbm.BackupMeta{}

	for i := 0; i < 1000; i++ {
		bcps = append(bcps, pbm.BackupMeta{
			Name: "bcp3",
			Replsets: []pbm.BackupReplset{
				{Name: "config"},
				{Name: "rs1"},
				{Name: "rs2"},
			},
			Status: pbm.StatusDone,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bcpsMatchCluster(bcps, shards, "config", nil)
	}
}

func BenchmarkBcpMatchCluster1000x1000(b *testing.B) {
	shards := []pbm.Shard{{RS: "config"}}
	rss := []pbm.BackupReplset{{Name: "config"}}

	for i := 0; i < 1000; i++ {
		shards = append(shards, pbm.Shard{RS: fmt.Sprint(i)})
		rss = append(rss, pbm.BackupReplset{Name: fmt.Sprint(i)})
	}

	bcps := []pbm.BackupMeta{}

	for i := 0; i < 1000; i++ {
		bcps = append(bcps, pbm.BackupMeta{
			Replsets: rss,
			Status:   pbm.StatusDone,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bcpsMatchCluster(bcps, shards, "config", nil)
	}
}

func BenchmarkBcpMatchCluster3x10Err(b *testing.B) {
	shards := []pbm.Shard{
		{RS: "config"},
		{RS: "rs2"},
	}

	bcps := []pbm.BackupMeta{
		{
			Name: "bcp1",
			Replsets: []pbm.BackupReplset{
				{Name: "config"},
				{Name: "rs1"},
				{Name: "rs3"},
			},
			Status: pbm.StatusDone,
		},
		{
			Name: "bcp2",
			Replsets: []pbm.BackupReplset{
				{Name: "config"},
				{Name: "rs1"},
			},
			Status: pbm.StatusDone,
		},
	}
	for i := 0; i < 8; i++ {
		bcps = append(bcps, pbm.BackupMeta{
			Replsets: []pbm.BackupReplset{
				{Name: "config"},
				{Name: "rs1"},
				{Name: "rs3"},
			},
			Status: pbm.StatusDone,
		})
	}
	for i := 0; i < b.N; i++ {
		bcpsMatchCluster(bcps, shards, "config", nil)
	}
}

func BenchmarkBcpMatchCluster1000x1000Err(b *testing.B) {
	shards := []pbm.Shard{{RS: "config"}}
	rss := []pbm.BackupReplset{{Name: "config"}}

	for i := 0; i < 1000; i++ {
		shards = append(shards, pbm.Shard{RS: fmt.Sprint(i)})
		rss = append(rss, pbm.BackupReplset{Name: fmt.Sprint(i)})
	}

	rss = append(rss, pbm.BackupReplset{Name: "newrs"})
	bcps := []pbm.BackupMeta{}

	for i := 0; i < 1000; i++ {
		bcps = append(bcps, pbm.BackupMeta{
			Replsets: rss,
			Status:   pbm.StatusDone,
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bcpsMatchCluster(bcps, shards, "config", nil)
	}
}
