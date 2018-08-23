package hotbackup

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/globalsign/mgo/dbtest"
)

const (
	testBackupPath = "testdata"
	testDBPath     = "testdata/dbpath"
)

func TestHotBackupNewBackup(t *testing.T) {
	checkHotBackupTest(t)

	cleanupDBPath(t)
	defer os.RemoveAll(testDBPath)

	var server dbtest.DBServer
	dbpath, _ := filepath.Abs(testDBPath)
	server.SetPath(dbpath)
	server.SetEngine("wiredTiger")
	defer server.Stop()

	session := server.Session()
	defer session.Close()

	backupName := "primary-" + strconv.Itoa(int(time.Now().Unix()))
	backupBase, _ := filepath.Abs(testBackupPath)
	backupDir := filepath.Join(backupBase, backupName)

	// this backup should succeed. use docker volume mount to check
	// the hot backup dir was created
	if _, err := os.Stat(backupDir); err == nil {
		t.Fatalf("Backup dir should not exist before backup: %s", backupDir)
	}
	b, err := NewBackup(session, backupDir)
	if err != nil {
		t.Fatalf("Failed to run .New(): %v", err.Error())
	} else if b.removed || b.dir == "" {
		t.Fatal("Got unexpected output from .New()")
	} else if _, err := os.Stat(backupDir); os.IsNotExist(err) {
		t.Fatalf("Cannot find backup dir after backup: %s", backupDir)
	} else if _, err := os.Stat(filepath.Join(backupDir, "storage.bson")); os.IsNotExist(err) {
		t.Fatalf("Cannot fine storage.bson file in backup dir: %s", backupDir)
	}
	defer b.Remove()

	// this should fail because the backup path already exists
	cleanupDBPath(t)
	_, err = NewBackup(session, backupDir)
	if err == nil {
		t.Fatal("Expected failure from .New() on second attempt")
	}
}

// TODO: use a real Hot Backup instead of simulation
func TestHotBackupDir(t *testing.T) {
	b := &Backup{dir: "/dev/null"}
	if b.Dir() != "/dev/null" {
		t.Fatal("Unexpected output from .Dir()")
	}
}

// TODO: use a real Hot Backup instead of simulation
func TestHotBackupRemove(t *testing.T) {
	tempDir, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("Could not create temp dir: %v", err.Error())
	}
	defer os.RemoveAll(tempDir)

	b := &Backup{dir: tempDir}
	err = b.Remove()
	if err != nil {
		t.Fatalf("Failed to run .Remove(): %v", err.Error())
	} else if _, err := os.Stat(tempDir); err == nil {
		t.Fatal("Backup dir should not exist after .Remove()")
	} else if !b.removed {
		t.Fatal("'removed' field should be true after .Remove()")
	} else if b.Remove() != nil {
		t.Fatal("Failed to run .Remove()")
	}
}

// TODO: use a real Hot Backup instead of simulation
func TestHotBackupClose(t *testing.T) {
	tempDir, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("Could not create temp dir: %v", err.Error())
	}
	defer os.RemoveAll(tempDir)

	b := &Backup{dir: tempDir}
	b.Close()

	if !b.removed {
		t.Fatal("'removed' field should be true after .Close()")
	} else if b.dir != "" {
		t.Fatal("'dir' field should be empty after .Close()")
	} else if _, err := os.Stat(tempDir); err == nil {
		t.Fatal("Backup dir should not exist after .Close()")
	}
}
