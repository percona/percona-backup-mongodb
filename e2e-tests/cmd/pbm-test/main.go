package main

import (
	"log"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/tests/sharded"
)

func main() {
	tests := sharded.New()

	tests.DeleteBallast()
	tests.GenerateBallastData(1e5)

	printStart("Basic Backup & Restore")
	tests.BackupAndRestore()
	printDone("Basic Backup & Restore")

	printStart("Backup Data Bounds Check")
	tests.BackupBoundsCheck()
	printDone("Backup Data Bounds Check")
}

func printStart(name string) {
	log.Printf("====== %s START ======\n", name)
}
func printDone(name string) {
	log.Printf("====== %s DONE ======\n", name)
}
