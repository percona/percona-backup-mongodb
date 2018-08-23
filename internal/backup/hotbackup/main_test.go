package hotbackup

import (
	"os"
	"testing"
)

var (
	testDB        = "test"
	testColl      = "test"
	defaultMongod = "/usr/bin/mongod"
)

func checkHotBackupTest(t *testing.T) {
	testMongod := os.Getenv("TEST_MONGODB_MONGOD")
	if testMongod == "" {
		testMongod = defaultMongod
	}
	if _, err := os.Stat(testMongod); os.IsNotExist(err) {
		t.Skipf("Skipping hotbackup test, %s (env var TEST_MONGODB_MONGOD) does not exist", testMongod)
	}
}
