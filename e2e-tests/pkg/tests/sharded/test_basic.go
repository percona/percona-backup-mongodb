package sharded

import (
	"log"

	"github.com/percona/percona-backup-mongodb/v2/pbm"
)

func (c *Cluster) BackupAndRestore(typ pbm.BackupType) {
	backup := c.LogicalBackup
	restore := c.LogicalRestore
	if typ == pbm.PhysicalBackup {
		backup = c.PhysicalBackup
		restore = c.PhysicalRestore
	}

	checkData := c.DataChecker()

	bcpName := backup()
	c.BackupWaitDone(bcpName)
	c.DeleteBallast()

	// to be sure the backup didn't vanish after the resync
	// i.e. resync finished correctly
	log.Println("resync backup list")
	err := c.mongopbm.StoreResync()
	if err != nil {
		log.Fatalln("Error: resync backup lists:", err)
	}

	restore(bcpName)
	checkData()
}
