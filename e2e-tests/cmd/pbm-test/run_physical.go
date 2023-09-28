package main

import (
	"math/rand"
	"time"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/tests/sharded"
	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/defs"
)

func runPhysical(t *sharded.Cluster, typ testTyp) {
	cVersion := majmin(t.ServerVersion())

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
		if !confExt(stg.conf) {
			continue
		}

		t.ApplyConfig(context.TODO(), stg.conf)
		flush(t)

		t.SetBallastData(1e5)

		runTest("Physical Backup & Restore "+stg.name,
			func() { t.BackupAndRestore(defs.PhysicalBackup) })

		flushStore(t)
	}

	runTest("Physical Backup Data Bounds Check",
		func() { t.BackupBoundsCheck(defs.PhysicalBackup, cVersion) })

	runTest("Incremental Backup & Restore ",
		func() { t.IncrementalBackup(cVersion) })

	if typ == testsSharded {
		runTest("Physical Distributed Transactions backup",
			t.DistributedTrxPhysical)
	}

	runTest("Clock Skew Tests",
		func() { t.ClockSkew(defs.PhysicalBackup, cVersion) })

	flushStore(t)
}
