package main

import (
	"log"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/tests/sharded"
)

func main() {
	tests := sharded.New(sharded.ClusterConf{
		Mongos:    "mongodb://dba:test1234@mongos:27017/",
		Configsrv: "mongodb://dba:test1234@cfg01:27017/",
		Shards: map[string]string{
			"rs1": "mongodb://dba:test1234@rs101:27017/",
			"rs2": "mongodb://dba:test1234@rs201:27017/",
		},
		DockerSocket: "unix:///var/run/docker.sock",
	})

	tests.DeleteBallast()
	tests.GenerateBallastData(1e5)

	printStart("Basic Backup & Restore")
	tests.BackupAndRestore()
	printDone("Basic Backup & Restore")

	printStart("Backup Data Bounds Check")
	tests.BackupBoundsCheck()
	printDone("Backup Data Bounds Check")

	printStart("Clock Skew Tests")
	tests.ClockSkew()
	printDone("Clock Skew Tests")

	printStart("Restart agents during the backup")
	tests.RestartAgents()
	printDone("Restart agents during the backup")

	printStart("Cut network during the backup")
	tests.NetworkCut()
	printDone("Cut network during the backup")
}

func printStart(name string) {
	log.Printf("[START] ======== %s ========\n", name)
}
func printDone(name string) {
	log.Printf("[DONE] ======== %s ========\n", name)
}
