package sharded

import (
	"log"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func (c *Cluster) NetworkCut() {
	log.Println("peek a random replset")
	rs := ""
	for name := range c.shards {
		rs = name
		break
	}
	if rs == "" {
		log.Fatalln("no shards in cluster")
	}

	bcpName := c.Backup()
	time.Sleep(time.Second * 2)

	log.Println("Cut network on agents", rs)
	err := c.docker.RunOnReplSet(rs, time.Second*5,
		"tc", "qdisc", "add", "dev", "eth0", "root", "netem", "loss", "100%",
	)
	if err != nil {
		log.Fatalf("ERROR: run tc netem on %s: %v", rs, err)
	}

	waitfor := time.Duration(pbm.StaleFrameSec+5) * time.Second
	log.Println("Sleeping for", waitfor)
	time.Sleep(waitfor)

	meta, err := c.mongopbm.GetBackupMeta(bcpName)
	if err != nil {
		log.Fatalf("ERROR: get metadata for the backup %s: %v", bcpName, err)
	}

	if meta.Status != pbm.StatusError && meta.Error != pbmLostAgentsErr {
		log.Fatalf("ERROR: wrong state of the backup %s. Expect: %s/%s. Got: %s/%s", bcpName, pbm.StatusError, pbmLostAgentsErr, meta.Status, meta.Error)
	}
	log.Printf("Backup status %s/%s\n", meta.Status, meta.Error)

	log.Println("Restore network on agents", rs)
	err = c.docker.RunOnReplSet(rs, time.Second*5,
		"tc", "qdisc", "del", "dev", "eth0", "root",
	)
	if err != nil {
		log.Fatalf("ERROR: run tc netem on %s: %v", rs, err)
	}
}
