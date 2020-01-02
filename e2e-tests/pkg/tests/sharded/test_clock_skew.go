package sharded

import (
	"log"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/pbm"
)

func (c *Cluster) ClockSkew() {
	log.Println("skew the clock for", "rs1", "+1h")
	err := pbm.ClockSkew("rs2", "+1h", "unix:///var/run/docker.sock")
	if err != nil {
		log.Fatalln("ERROR: clock skew for rs1:", err)
	}
	c.BackupAndRestore()
}
