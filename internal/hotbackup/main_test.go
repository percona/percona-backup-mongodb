package hotbackup

import (
	"os"
	"testing"
)

func checkHotBackupTest(t *testing.T) {
	if os.Getenv("TEST_MONGODB_HOTBACKUP") != "true" {
		t.Skip("Skipping hotbackup test, TEST_MONGODB_HOTBACKUP is not 'true'")
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
