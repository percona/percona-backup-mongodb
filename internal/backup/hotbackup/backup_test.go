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

func TestHotBackupNewBackupWiredTiger(t *testing.T) {
	t.Skip("Sandbox does not support hotback")
	checkHotBackupTest(t)

	tmpDBPath, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("Failed to create temp dbpath: %v", err.Error())
	}
	defer os.RemoveAll(tmpDBPath)

	var server dbtest.DBServer
	server.SetPath(tmpDBPath)
	server.SetEngine("wiredTiger")
	defer server.Stop()

	session := server.Session()
	defer session.Close()

	backupBase, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("Failed to create backup temp dir: %v", err.Error())
	}
	defer os.RemoveAll(backupBase)

	backupName := "primary-" + strconv.Itoa(int(time.Now().Unix()))
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
	_, err = NewBackup(session, backupDir)
	if err == nil {
		t.Fatal("Expected failure from .New() on second attempt")
	}
}

func TestHotBackupNewBackupMMAPv1(t *testing.T) {
	t.Skip("Sandbox does not support hotback")
	// this should fail because mmapv1 is used
	tmpDBPath, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("Failed to create backup temp dir: %v", err.Error())
	}
	defer os.RemoveAll(tmpDBPath)

	var server dbtest.DBServer
	server.SetPath(tmpDBPath)
	if err != nil {
		t.Fatalf("Failed to create backup temp dir: %v", err.Error())
	}
	server.SetEngine("mmapv1")
	defer server.Stop()

	session := server.Session()
	defer session.Close()

	_, err = NewBackup(session, "/data/db/.backup")
	if err == nil || err.Error() != ErrMsgUnsupportedEngine {
		t.Fatal(".NewBackup() should return an unsupported engine error for mmapv1")
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
