package hotbackup

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/globalsign/mgo/dbtest"
)

var ()

func TestHotBackupStopServer(t *testing.T) {
	if os.Getenv("TEST_MONGODB_HOTBACKUP") != "true" {
		t.Skip("Skipping hotbackup test, TEST_MONGODB_HOTBACKUP is not 'true'")
	}

	if _, err := os.Stat(testDBPath); os.IsNotExist(err) {
		err := os.MkdirAll(testDBPath, 0777)
		if err != nil {
			t.Fatalf("Cannot make test dir %s: %v", testDBPath, err.Error())
		}
	}
	defer os.RemoveAll(testDBPath)

	var server dbtest.DBServer
	dbpath, _ := filepath.Abs(testDBPath)
	server.SetPath(dbpath)
	defer server.Stop()

	session := server.Session()
	defer session.Close()

	restore, err := NewRestore(session, testDBPath)
	if err != nil {
		t.Fatalf("Failed to run .NewRestore(): %v", err.Error())
	}

	err = restore.stopServer()
	if err != nil {
		t.Fatalf("Failed to run .stopServer(): %v", err.Error())
	}
}
