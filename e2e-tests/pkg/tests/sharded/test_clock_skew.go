package sharded

import (
	"log"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/pbm"
	pbmt "github.com/percona/percona-backup-mongodb/pbm"
)

func (c *Cluster) ClockSkew(typ pbmt.BackupType, mongoVersion string) {
	timeShifts := []string{
		"+90m", "-195m", "+2d", "-7h", "+11m", "+42d", "-13h",
	}
	rsNames := []string{"cfg"}
	for s := range c.shards {
		rsNames = append(rsNames, s)
	}

	for k, rs := range rsNames {
		if k >= len(timeShifts) {
			k = k % len(timeShifts)
		}
		shift := timeShifts[k]

		err := pbm.ClockSkew(rs, shift, c.cfg.DockerURI)
		if err != nil {
			log.Fatalf("ERROR: clock skew for %v: %v\n", rs, err)
		}

		log.Printf("[START] %s Backup Data Bounds Check / ClockSkew %s %s", typ, rs, shift)
		c.BackupBoundsCheck(typ, mongoVersion)
		log.Printf("[DONE] %s Backup Data Bounds Check / ClockSkew %s %s", typ, rs, shift)
	}
}
