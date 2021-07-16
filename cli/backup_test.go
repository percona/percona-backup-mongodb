package cli

import (
	"fmt"
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
			bcpsMatchCluster(m, c.shards, c.confsrv)
			for i := 0; i < len(c.bcps); i++ {
				if c.bcps[i].expect != m[i].Status {
					t.Errorf("wrong status for %s, expect %s, got %s", m[i].Name, c.bcps[i].expect, m[i].Status)
				}
			}
		})
	}
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
		bcpsMatchCluster(bcps, shards, "config")
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
		bcpsMatchCluster(bcps, shards, "config")
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
		bcpsMatchCluster(bcps, shards, "config")
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
		bcpsMatchCluster(bcps, shards, "config")
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
		bcpsMatchCluster(bcps, shards, "config")
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
		bcpsMatchCluster(bcps, shards, "config")
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
		bcpsMatchCluster(bcps, shards, "config")
	}
}
