package main

import (
	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/tests/sharded"
)

func main() {
	tests := sharded.New()

	tests.BackupAndRestore()
}
