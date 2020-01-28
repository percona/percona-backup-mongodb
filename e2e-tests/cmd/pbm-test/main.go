package main

import (
	"log"
	"time"

	"github.com/hashicorp/go-version"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/tests/sharded"
)

func main() {
	tests := sharded.New(sharded.ClusterConf{
		Mongos:          "mongodb://dba:test1234@mongos:27017/",
		Configsrv:       "mongodb://dba:test1234@cfg01:27017/",
		ConfigsrvRsName: "cfg",
		Shards: map[string]string{
			"rs1": "mongodb://dba:test1234@rs101:27017/",
			"rs2": "mongodb://dba:test1234@rs201:27017/",
		},
		DockerSocket: "unix:///var/run/docker.sock",
	})

	// tests.ApplyConfig("/etc/pbm/store.yaml")

	tests.DeleteBallast()
	tests.GenerateBallastData(1e5)

	// printStart("Basic Backup & Restore AWS S3")
	// tests.BackupAndRestore()
	// printDone("Basic Backup & Restore AWS S3")

	tests.ApplyConfig("/etc/pbm/minio.yaml")
	log.Println("Waiting for the new storage to resync")
	time.Sleep(time.Second * 5)

	printStart("Basic Backup & Restore")
	tests.BackupAndRestore()
	printDone("Basic Backup & Restore")

	printStart("Backup Data Bounds Check")
	tests.BackupBoundsCheck()
	printDone("Backup Data Bounds Check")

	printStart("Restart agents during the backup")
	tests.RestartAgents()
	printDone("Restart agents during the backup")

	printStart("Cut network during the backup")
	tests.NetworkCut()
	printDone("Cut network during the backup")

	cVersion := version.Must(version.NewVersion(tests.ServerVersion()))
	v42 := version.Must(version.NewVersion("4.2"))

	if cVersion.GreaterThanOrEqual(v42) {
		printStart("Distributed Transactions backup")
		tests.DistributedTransactions()
		printDone("Distributed Transactions backup")
	}

	printStart("Clock Skew Tests")
	tests.ClockSkew()
	printDone("Clock Skew Tests")
}

func printStart(name string) {
	log.Printf("[START] ======== %s ========\n", name)
}
func printDone(name string) {
	log.Printf("[DONE] ======== %s ========\n", name)
}
