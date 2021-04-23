package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"

	"github.com/minio/minio-go"
	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/tests/sharded"
	"github.com/percona/percona-backup-mongodb/pbm"
	"golang.org/x/mod/semver"
	"gopkg.in/yaml.v2"
)

func run(t *sharded.Cluster, typ testTyp) {
	storage := "/etc/pbm/aws.yaml"
	if confExt(storage) {
		flushStore(storage)
		t.ApplyConfig(storage)

		t.SetBallastData(1e5)

		printStart("Basic Backup & Restore AWS S3")
		t.BackupAndRestore()
		printDone("Basic Backup & Restore AWS S3")

		printStart("Basic PITR & Restore AWS")
		t.PITRbasic()
		printDone("Basic PITR & Restore AWS")

		printStart("Check Backups deletion AWS")
		t.BackupDelete(storage)
		printDone("Check Backups deletion AWS")

		flushStore(storage)
	}

	storage = "/etc/pbm/gcs.yaml"
	if confExt(storage) {
		flushStore(storage)
		t.ApplyConfig(storage)

		t.SetBallastData(1e5)

		printStart("Basic Backup & Restore GCS")
		t.BackupAndRestore()
		printDone("Basic Backup & Restore GCS")

		printStart("Basic PITR & Restore GCS")
		t.PITRbasic()
		printDone("Basic PITR & Restore GCS")

		printStart("Check Backups deletion GCS")
		t.BackupDelete(storage)
		printDone("Check Backups deletion GCS")

		flushStore(storage)
	}

	storage = "/etc/pbm/azure.yaml"
	if confExt(storage) {
		flushStore(storage)
		t.ApplyConfig(storage)

		t.SetBallastData(1e5)

		printStart("Basic Backup & Restore Azure")
		t.BackupAndRestore()
		printDone("Basic Backup & Restore Azure")

		printStart("Basic PITR & Restore Azure")
		t.PITRbasic()
		printDone("Basic PITR & Restore Azure")

		printStart("Check Backups deletion Azure")
		t.BackupDelete(storage)
		printDone("Check Backups deletion Azure")

		flushStore(storage)
	}

	storage = "/etc/pbm/fs.yaml"

	flushStore(storage)
	t.ApplyConfig(storage)

	t.SetBallastData(1e5)

	printStart("Basic Backup & Restore FS")
	t.BackupAndRestore()
	printDone("Basic Backup & Restore FS")

	printStart("Basic PITR & Restore FS")
	t.PITRbasic()
	printDone("Basic PITR & Restore FS")

	flushStore(storage)

	storage = "/etc/pbm/minio.yaml"

	flushStore(storage)
	t.ApplyConfig(storage)

	t.SetBallastData(1e5)

	printStart("Basic Backup & Restore Minio")
	t.BackupAndRestore()
	printDone("Basic Backup & Restore Minio")

	printStart("Basic PITR & Restore Minio")
	t.PITRbasic()
	printDone("Basic PITR & Restore Minio")

	t.SetBallastData(1e3)
	flushStore(storage)

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

		cVersion := majmin(t.ServerVersion())
		if semver.Compare(cVersion, "v4.2") >= 0 {
			printStart("Distributed Transactions backup")
			t.DistributedTrxSnapshot()
			printDone("Distributed Transactions backup")

			printStart("Distributed Transactions PITR")
			t.DistributedTrxPITR()
			printDone("Distributed Transactions PITR")
		}
	}

	printStart("Clock Skew Tests")
	t.ClockSkew()
	printDone("Clock Skew Tests")

	flushStore(storage)
}

func printStart(name string) {
	log.Printf("[START] ======== %s ========\n", name)
}
func printDone(name string) {
	log.Printf("[DONE] ======== %s ========\n", name)
}

const awsurl = "s3.amazonaws.com"

func flushStore(conf string) {
	buf, err := ioutil.ReadFile(conf)
	if err != nil {
		log.Fatalln("Error: unable to read config file:", err)
	}

	var cfg pbm.Config
	err = yaml.UnmarshalStrict(buf, &cfg)
	if err != nil {
		log.Fatalln("Error: unmarshal yaml:", err)
	}

	stg := cfg.Storage

	endopintURL := awsurl
	if stg.S3.EndpointURL != "" {
		eu, err := url.Parse(stg.S3.EndpointURL)
		if err != nil {
			log.Fatalln("Error: parse EndpointURL:", err)
		}
		endopintURL = eu.Host
	}

	log.Println("Flushing store", endopintURL, stg.S3.Bucket, stg.S3.Prefix)

	mc, err := minio.NewWithRegion(endopintURL, stg.S3.Credentials.AccessKeyID, stg.S3.Credentials.SecretAccessKey, false, stg.S3.Region)
	if err != nil {
		log.Fatalln("Error: NewWithRegion:", err)
	}

	objectsCh := make(chan string)

	go func() {
		defer close(objectsCh)
		for object := range mc.ListObjects(stg.S3.Bucket, stg.S3.Prefix, true, nil) {
			if object.Err != nil {
				fmt.Fprintln(os.Stderr, "Error: ListObjects:", object.Err)
				continue
			}
			objectsCh <- object.Key
		}
	}()

	for rErr := range mc.RemoveObjects(stg.S3.Bucket, objectsCh) {
		fmt.Fprintln(os.Stderr, "Error detected during deletion:", rErr)
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
