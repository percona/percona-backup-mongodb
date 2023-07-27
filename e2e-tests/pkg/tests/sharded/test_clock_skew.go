package sharded

import (
	"log"

	pbmt "github.com/percona/percona-backup-mongodb/e2e-tests/pkg/pbm"
	"github.com/percona/percona-backup-mongodb/pbm"
)

func (c *Cluster) ClockSkew(typ pbm.BackupType, mongoVersion string) {
	timeShifts := []string{
		"+90m", "-195m", "+2d", "-7h", "+11m", "+42d", "-13h",
	}
	rsNames := []string{"cfg"}
	for s := range c.shards {
		rsNames = append(rsNames, s)
	}

	for k, rs := range rsNames {
		if k >= len(timeShifts) {
			k %= len(timeShifts)
		}
		shift := timeShifts[k]

		err := pbmt.ClockSkew(rs, shift, c.cfg.DockerURI)
		if err != nil {
			log.Fatalf("ERROR: clock skew for %v: %v\n", rs, err)
		}

		log.Printf("[START] %s Backup Data Bounds Check / ClockSkew %s %s", typ, rs, shift)
		c.BackupBoundsCheck(typ, mongoVersion)
		log.Printf("[DONE] %s Backup Data Bounds Check / ClockSkew %s %s", typ, rs, shift)
	}
}
