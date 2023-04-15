package main

import (
	"math/rand"
	"time"
	"golang.org/x/mod/semver"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/tests/sharded"
	"github.com/percona/percona-backup-mongodb/pbm"
)

func runPhysical(t *sharded.Cluster, typ testTyp) {
	cVersion := majmin(t.ServerVersion())

	storage := "/etc/pbm/fs.yaml"
	// t.ApplyConfig(storage)
	// flush(t)

	remoteStg := []struct {
		name string
		conf string
	}{
		{"AWS", "/etc/pbm/aws.yaml"},
		{"GCS", "/etc/pbm/gcs.yaml"},
		{"Azure", "/etc/pbm/azure.yaml"},
		{"FS", "/etc/pbm/fs.yaml"},
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(remoteStg), func(i, j int) {
		remoteStg[i], remoteStg[j] = remoteStg[j], remoteStg[i]
	})

	minio := struct {
		name string
		conf string
	}{name: "Minio", conf: "/etc/pbm/minio.yaml"}
	remoteStg = append(remoteStg, minio)

	for _, stg := range remoteStg {
		if confExt(stg.conf) {
			storage = stg.conf

			t.ApplyConfig(storage)
			flush(t)

			t.SetBallastData(1e5)

			runTest("Physical Backup & Restore "+stg.name,
				func() { t.BackupAndRestore(pbm.PhysicalBackup) })

			flushStore(t)
		}
	}

	runTest("Physical Backup Data Bounds Check",
		func() { t.BackupBoundsCheck(pbm.PhysicalBackup, cVersion) })

	runTest("Incremental Backup & Restore ",
		func() { t.IncrementalBackup(cVersion) })

	if typ == testsSharded {
		if semver.Compare(cVersion, "v4.2") >= 0 {
			runTest("Physical Distributed Transactions backup",
				t.DistributedTrxPhysical)
		}
	}

	runTest("Clock Skew Tests",
		func() { t.ClockSkew(pbm.PhysicalBackup, cVersion) })

	flushStore(t)
}
