package sharded

import (
	"log"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm"
)

const pbmLostAgentsErr = "some pbm-agents were lost during the backup"

func (c *Cluster) RestartAgents() {
	log.Println("peek random replset")
	rs := ""
	for name := range c.shards {
		rs = name
		break
	}
	if rs == "" {
		log.Fatalln("no shards in cluster")
	}

	bcpName := c.Backup()
	log.Println("Stopping agents on the replset", rs)
	err := c.docker.StopAgents(rs)
	if err != nil {
		log.Fatalln("ERROR: stopping agents on the replset", err)
	}
	log.Println("Agents has stopped", rs)

	waitfor := time.Duration(pbm.StaleFrameSec+5) * time.Second
	log.Println("Sleeping for", waitfor)
	time.Sleep(waitfor)
	meta, err := c.mongopbm.GetBackupMeta(bcpName)
	if err != nil {
		log.Fatalf("ERROR: get metadata for the backup %s: %v", bcpName, err)
	}

	log.Println("Starting agents on the replset", rs)
	err = c.docker.StartAgents(rs)
	if err != nil {
		log.Fatalln("ERROR: starting agents on the replset", err)
	}
	log.Println("Agents started", rs)

	if meta.Status != pbm.StatusError && meta.Error != pbmLostAgentsErr {
		log.Fatalf("ERROR: wrong state of the backup %s. Expect: %s/%s. Got: %s/%s", bcpName, pbm.StatusError, pbmLostAgentsErr, meta.Status, meta.Error)
	}

	log.Println("Trying a new backup")
	c.BackupAndRestore()
}
