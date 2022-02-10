package main

import (
	"fmt"
	"log"
	"os"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/tests/sharded"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"golang.org/x/mod/semver"
)

func run(t *sharded.Cluster, typ testTyp) {
	cVersion := majmin(t.ServerVersion())

	remoteStg := []struct {
		name string
		conf string
	}{
		{"AWS", "/etc/pbm/aws.yaml"},
		{"GCS", "/etc/pbm/gcs.yaml"},
		{"Azure", "/etc/pbm/azure.yaml"},
	}

	for _, stg := range remoteStg {
		if confExt(stg.conf) {
			t.ApplyConfig(stg.conf)
			flush(t)

			t.SetBallastData(1e5)

			printStart("Basic Backup & Restore " + stg.name)
			t.BackupAndRestore()
			printDone("Basic Backup & Restore " + stg.name)

			printStart("Basic PITR & Restore " + stg.name)
			t.PITRbasic()
			printDone("Basic PITR & Restore " + stg.name)

			t.SetBallastData(1e3)
			flush(t)

			printStart("Check Backups deletion " + stg.name)
			t.BackupDelete(stg.conf)
			printDone("Check Backups deletion " + stg.name)

			flushStore(t)
		}
	}

	storage := "/etc/pbm/fs.yaml"

	t.ApplyConfig(storage)
	flush(t)

	t.SetBallastData(1e5)

	printStart("Basic Backup & Restore FS")
	t.BackupAndRestore()
	printDone("Basic Backup & Restore FS")

	printStart("Basic PITR & Restore FS")
	t.PITRbasic()
	printDone("Basic PITR & Restore FS")

	storage = "/etc/pbm/minio.yaml"

	t.ApplyConfig(storage)
	flush(t)

	printStart("Basic Backup & Restore Minio")
	t.BackupAndRestore()
	printDone("Basic Backup & Restore Minio")

	printStart("Basic PITR & Restore Minio")
	t.PITRbasic()
	printDone("Basic PITR & Restore Minio")

	t.SetBallastData(1e3)
	flush(t)

	if semver.Compare(cVersion, "v5.0") >= 0 {
		printStart("Check timeseries")
		t.Timeseries()
		printDone("Check timeseries")
		flush(t)
	}

	printStart("Check Backups deletion")
	t.BackupDelete(storage)
	printDone("Check Backups deletion")

	t.SetBallastData(1e5)

	printStart("Check the Running Backup can't be deleted")
	t.BackupNotDeleteRunning()
	printDone("Check the Running Backup can't be deleted")

	printStart("Check Backup Cancellation")
	t.BackupCancellation(storage)
	printDone("Check Backup Cancellation")

	printStart("Leader lag during backup start")
	t.LeaderLag()
	printDone("Leader lag during backup start")

	printStart("Backup Data Bounds Check")
	t.BackupBoundsCheck()
	printDone("Backup Data Bounds Check")

	if typ == testsSharded {
		t.SetBallastData(1e6)

		// TODO: in the case of non-sharded cluster there is no other agent to observe
		// TODO: failed state during the backup. For such topology test should check if
		// TODO: a sequential run (of the backup let's say) handles a situation.
		printStart("Cut network during the backup")
		t.NetworkCut()
		printDone("Cut network during the backup")

		t.SetBallastData(1e5)

		printStart("Restart agents during the backup")
		t.RestartAgents()
		printDone("Restart agents during the backup")

		if semver.Compare(cVersion, "v4.2") >= 0 {
			printStart("Distributed Transactions backup")
			t.DistributedTrxSnapshot()
			printDone("Distributed Transactions backup")

			printStart("Distributed Transactions PITR")
			t.DistributedTrxPITR()
			printDone("Distributed Transactions PITR")
		}

		if semver.Compare(cVersion, "v4.4") >= 0 {
			disttxnconf := "/etc/pbm/fs-disttxn-4x.yaml"
			tsTo := primitive.Timestamp{1644410656, 8}

			if semver.Compare(cVersion, "v5.0") >= 0 {
				disttxnconf = "/etc/pbm/fs-disttxn-50.yaml"
				tsTo = primitive.Timestamp{1644243375, 7}
			}
			t.ApplyConfig(disttxnconf)
			printStart("Distributed Commit")
			t.DistributedCommit(tsTo)
			printDone("Distributed Commit")
			t.ApplyConfig(storage)
		}
	}

	printStart("Clock Skew Tests")
	t.ClockSkew()
	printDone("Clock Skew Tests")

	flushStore(t)
}

func printStart(name string) {
	log.Printf("[START] ======== %s ========\n", name)
}
func printDone(name string) {
	log.Printf("[DONE] ======== %s ========\n", name)
}

const awsurl = "s3.amazonaws.com"

func flush(t *sharded.Cluster) {
	flushStore(t)
	flushPbm(t)
}

func flushPbm(t *sharded.Cluster) {
	err := t.Flush()
	if err != nil {
		log.Fatalln("Error: unable flush pbm db:", err)
	}
}

func flushStore(t *sharded.Cluster) {
	err := t.FlushStorage()
	if err != nil {
		log.Fatalln("Error: unable flush storage:", err)
	}
}

func confExt(f string) bool {
	_, err := os.Stat(f)
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error checking config %s: %v\n", f, err)
		return false
	}

	return true
}

func majmin(v string) string {
	if len(v) == 0 {
		return v
	}

	if v[0] != 'v' {
		v = "v" + v
	}

	return semver.MajorMinor(v)
}
