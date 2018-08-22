package hotbackup

import (
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/globalsign/mgo/dbtest"
)

const (
	testWiredTigerHotBackupArchive = "testdata/wiredTiger-HotBackup.tar.gz"
	testRestorePath                = "testdata/restore"
)

func TestHotBackupRestoreStopServer(t *testing.T) {
	checkHotBackupTest(t)

	cleanupDBPath(t)
	defer os.RemoveAll(testDBPath)

	var server dbtest.DBServer
	dbpath, _ := filepath.Abs(testDBPath)
	server.SetPath(dbpath)
	server.SetMonitor(false)

	session := server.Session()
	defer session.Close()

	restore, err := NewRestore(session, testRestorePath, testDBPath)
	if err != nil {
		t.Fatalf("Failed to run .NewRestore(): %v", err.Error())
	}

	err = restore.stopServer()
	if err != nil {
		t.Fatalf("Failed to run .stopServer(): %v", err.Error())
	} else if len(restore.serverArgv) <= 1 {
		t.Fatal("Server argv is not greater than 1")
	} else if !restore.serverShutdown {
		t.Fatal("Server serverShutdown flag is not true")
	}
}

func TestHotBackupRestoreDBPath(t *testing.T) {
	checkHotBackupTest(t)
	cleanupDBPath(t)
	defer os.RemoveAll(testDBPath)

	err := os.MkdirAll(testRestorePath, 0777)
	if err != nil {
		t.Fatalf("Failed to setup restore dir: %v", err.Error())
	}
	defer os.RemoveAll(testRestorePath)

	// un-archive the hotbackup test archive file
	cmd := exec.Command("tar", "-C", testRestorePath, "-xzf", testWiredTigerHotBackupArchive)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to uncompress test dbpath: %v", err.Error())
	}

	// restore the hotbackup to the dbpath
	currentUser, _ := user.Current()
	uidInt, _ := strconv.Atoi(currentUser.Uid)
	uid := uint32(uidInt)
	restore := &Restore{backupPath: testRestorePath, dbPath: testDBPath, uid: &uid}
	err = restore.restoreDBPath()
	if err != nil {
		t.Fatalf("Failed to run .restoreDBPath(): %v", err.Error())
	} else if _, err := os.Stat(filepath.Join(testRestorePath, "storage.bson")); os.IsNotExist(err) {
		t.Fatal("Restored dbpath does not contain storage.bson!")
	}
}
