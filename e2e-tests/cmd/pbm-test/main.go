package main

import (
	"log"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/tests/sharded"
)

func main() {
	tests := sharded.New()

	// tests.DeleteData()
	// tests.GenerateBallastData(1e5)

	// log.Println("====== Basic Backup & Restore START ======")
	// tests.BackupAndRestore()
	// log.Println("====== Basic Backup & Restore DONE ======")

	log.Println("====== Check Backup Data Bounds START ======")
	tests.BackupCheckBounds()
	log.Println("====== Check Backup Data Bounds DONE ======")
}
