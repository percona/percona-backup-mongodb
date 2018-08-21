package hotbackup

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils"
)

const (
	testBackupRealPath      = "testdata/backup"
	testBackupContainerPath = "/data/backup"
)

var ()

func TestHotBackupNew(t *testing.T) {
	var fi os.FileInfo
	var err error
	var runTest bool
	if fi, err = os.Stat(testBackupContainerPath); err == nil {
		runTest = true
	} else if fi, err = os.Stat(testBackupRealPath); err == nil {
		runTest = true
	}
	if !runTest || !fi.IsDir() {
		t.Skipf("The directory %q (or %q) should be mounted on a docker volume. %s", testBackupContainerPath, testBackupRealPath, err)
	}

	session, err := mgo.DialWithInfo(testutils.PrimaryDialInfo())
	if err != nil {
		t.Fatalf("Failed to get primary session: %v", err.Error())
	}
	defer session.Close()

	backupName := "primary-" + strconv.Itoa(int(time.Now().Unix()))
	containerBackupDir := filepath.Join(testBackupContainerPath, backupName)
	realBackupDir := filepath.Join(testBackupRealPath, backupName)

	// this backup should succeed. use docker volume mount to check
	// the hot backup dir was created
	if _, err := os.Stat(realBackupDir); err == nil {
		t.Fatalf("Backup dir should not exist before backup: %s", realBackupDir)
	}
	b, err := NewBackup(session, containerBackupDir)
	if err != nil {
		t.Fatalf("Failed to run .New(): %v", err.Error())
	} else if b.removed || b.dir == "" {
		t.Fatal("Got unexpected output from .New()")
	} else if _, err := os.Stat(realBackupDir); os.IsNotExist(err) {
		t.Fatalf("Cannot find backup dir after backup: %s", realBackupDir)
	}

	// this should fail because the backup path already exists
	_, err = NewBackup(session, containerBackupDir)
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
