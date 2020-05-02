package sharded

import (
	"log"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func (c *Cluster) BackupCancellation(storage string) {
	bcpName := c.Backup()
	time.Sleep(1 * time.Second)
	c.printBcpList()
	log.Println("canceling backup", bcpName)
	o, err := c.pbm.RunCmd("pbm", "cancel-backup")
	if err != nil {
		log.Fatalf("Error: cancel backup '%s'.\nOutput: %s\nStderr:%v", bcpName, o, err)
	}

	time.Sleep(3 * time.Second)

	checkNoFiles(bcpName, storage)

	log.Println("check backup state")
	m, err := c.mongopbm.GetBackupMeta(bcpName)
	if err != nil {
		log.Fatalf("Error: get metadata for backup %s: %v", bcpName, err)
	}

	if m.Status != pbm.StatusCancelled {
		log.Fatalf("Error: wrong backup status, expect %s, got %v", pbm.StatusCancelled, m.Status)
	}
}
