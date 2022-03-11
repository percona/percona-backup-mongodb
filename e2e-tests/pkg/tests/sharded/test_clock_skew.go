package sharded

import (
	"log"

	pbmt "github.com/percona/percona-backup-mongodb/pbm"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/pbm"
)

func (c *Cluster) ClockSkew() {
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

		err := pbm.ClockSkew(rs, shift, "unix:///var/run/docker.sock")
		if err != nil {
			log.Fatalf("ERROR: clock skew for %v: %v\n", rs, err)
		}

		log.Println("[START] Logical Backup Data Bounds Check / ClockSkew", rs, shift)
		c.BackupBoundsCheck(pbmt.LogicalBackup)
		log.Println("[DONE] Logical Backup Data Bounds Check / ClockSkew", rs, shift)

		log.Println("[START] Physical Backup Data Bounds Check / ClockSkew", rs, shift)
		c.BackupBoundsCheck(pbmt.PhysicalBackup)
		log.Println("[DONE] Physical Backup Data Bounds Check / ClockSkew", rs, shift)
	}
}
