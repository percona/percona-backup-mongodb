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

func TestHotBackupNew(t *testing.T) {
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
	hb, err := New(session, containerBackupDir)
	if err != nil {
		t.Fatalf("Failed to run .New(): %v", err.Error())
	} else if hb.removed || hb.dir == "" {
		t.Fatal("Got unexpected output from .New()")
	} else if _, err := os.Stat(realBackupDir); os.IsNotExist(err) {
		t.Fatalf("Cannot find backup dir after backup: %s", realBackupDir)
	}

	// this should fail because the backup path already exists
	_, err = New(session, containerBackupDir)
	if err == nil {
		t.Fatal("Expected failure from .New() on second attempt")
	}
}

// TODO: use a real Hot Backup instead of simulation
func TestHotBackupDir(t *testing.T) {
	hb := &HotBackup{dir: "/dev/null"}
	if hb.Dir() != "/dev/null" {
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

	hb := &HotBackup{dir: tempDir}
	err = hb.Remove()
	if err != nil {
		t.Fatalf("Failed to run .Remove(): %v", err.Error())
	} else if _, err := os.Stat(tempDir); err == nil {
		t.Fatal("Backup dir should not exist after .Remove()")
	} else if !hb.removed {
		t.Fatal("'removed' field should be true after .Remove()")
	}
}

// TODO: use a real Hot Backup instead of simulation
func TestHotBackupClose(t *testing.T) {
	tempDir, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("Could not create temp dir: %v", err.Error())
	}
	defer os.RemoveAll(tempDir)

	hb := &HotBackup{dir: tempDir}
	hb.Close()

	if !hb.removed {
		t.Fatal("'removed' field should be true after .Close()")
	} else if hb.dir != "" {
		t.Fatal("'dir' field should be empty after .Close()")
	} else if _, err := os.Stat(tempDir); err == nil {
		t.Fatal("Backup dir should not exist after .Close()")
	}
}
