package hotbackup

import (
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

	// this should succeed
	hb, err := New(session, containerBackupDir)
	if err != nil {
		t.Fatalf("Failed to run .New(): %v", err.Error())
	} else if hb.removed || hb.backupDir == "" {
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
