package main

import (
	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/tests/sharded"
	"github.com/percona/percona-backup-mongodb/pbm"
)

func runRemappingTests(t *sharded.RemappingEnvironment) {
	storage := "/etc/pbm/minio.yaml"
	if confExt(storage) {
		t.Donor.ApplyConfig(storage)
		flush(t.Donor)
		t.Recipient.ApplyConfig(storage)
		flush(t.Recipient)

		t.Donor.SetBallastData(1e4)

		runTest("Logical Backup & Restore with remapping Minio",
			func() { t.BackupAndRestore(pbm.LogicalBackup) })

		flushStore(t.Recipient)
	}
}
