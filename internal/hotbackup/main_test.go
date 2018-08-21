package hotbackup

import (
	"os"
	"testing"
)

var DefaultMongod = "/usr/bin/mongod"

func checkHotBackupTest(t *testing.T) {
	testMongod := os.Getenv("TEST_MONGODB_MONGOD")
	if testMongod == "" {
		testMongod = DefaultMongod
	}
	if _, err := os.Stat(testMongod); os.IsNotExist(err) {
		t.Skipf("Skipping hotbackup test, %s (env var TEST_MONGODB_MONGOD) does not exist", testMongod)
	}
}

func cleanupDBPath(t *testing.T) {
	if _, err := os.Stat(testDBPath); os.IsNotExist(err) {
		err := os.MkdirAll(testDBPath, 0777)
		if err != nil {
			t.Fatalf("Cannot make test dbpath dir %s: %v", testDBPath, err.Error())
		}
	}
}
