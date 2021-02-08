package sharded

import (
	"log"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm"
)

// LeaderLag checks if cluster deals with leader lag during backup start
// https://jira.percona.com/browse/PBM-635
func (c *Cluster) LeaderLag() {
	checkData := c.DataChecker()

	log.Println("Pausing agents on", c.confsrv)
	err := c.docker.PauseAgents(c.confsrv)
	if err != nil {
		log.Fatalln("ERROR: pausing agents:", err)
	}
	log.Println("Agents on pause", c.confsrv)

	bcpName := time.Now().UTC().Format(time.RFC3339)

	log.Println("Starting backup", bcpName)
	err = c.mongopbm.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdBackup,
		Backup: pbm.BackupCmd{
			Name:        bcpName,
			Compression: pbm.CompressionTypeS2,
		},
	})
	if err != nil {
		log.Fatalln("ERROR: starting backup:", err)
	}

	waitfor := time.Second * 5
	log.Println("Sleeping for", waitfor)
	time.Sleep(waitfor)

	log.Println("Unpausing agents on", c.confsrv)
	err = c.docker.UnpauseAgents(c.confsrv)
	if err != nil {
		log.Fatalln("ERROR: unpause agents:", err)
	}
	log.Println("Agents resumed", c.confsrv)

	c.BackupWaitDone(bcpName)
	c.DeleteBallast()

	// to be sure the backup didn't vanish after the resync
	// i.e. resync finished correctly
	log.Println("resync backup list")
	err = c.mongopbm.StoreResync()
	if err != nil {
		log.Fatalln("Error: resync backup lists:", err)
	}

	c.Restore(bcpName)
	checkData()
}
