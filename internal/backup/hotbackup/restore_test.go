package hotbackup

import (
	"io/ioutil"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/globalsign/mgo/bson"
	"github.com/globalsign/mgo/dbtest"
)

const (
	testWiredTigerHotBackupArchive = "testdata/wiredTiger-HotBackup.tar.gz"
)

func TestHotBackupRestoreStopServer(t *testing.T) {
	checkHotBackupTest(t)

	tmpDBPath, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("Failed to create temp dbpath: %v", err.Error())
	}
	defer os.RemoveAll(tmpDBPath)

	var server dbtest.DBServer
	dbpath, _ := filepath.Abs(tmpDBPath)
	server.SetPath(dbpath)
	server.SetMonitor(false)

	session := server.Session()
	defer session.Close()

	restore, err := NewRestore(session, "", tmpDBPath)
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

	tmpDBPath, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("Failed to create temp dbpath: %v", err.Error())
	}
	defer os.RemoveAll(tmpDBPath)

	restoreTmpPath, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("Failed to create restore dir: %v", err.Error())
	}
	defer os.RemoveAll(restoreTmpPath)

	// un-archive the hotbackup test archive file
	cmd := exec.Command("tar", "-C", restoreTmpPath, "-xzf", testWiredTigerHotBackupArchive)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to uncompress test dbpath: %v", err.Error())
	}

	currentUser, _ := user.Current()
	uidInt, _ := strconv.Atoi(currentUser.Uid)
	uid := uint32(uidInt)
	restore := &Restore{
		backupPath: restoreTmpPath,
		dbPath:     tmpDBPath,
		uid:        &uid,
		lockFile:   filepath.Join(tmpDBPath, "mongod.lock"),
	}

	// restore the hotbackup to the dbpath
	err = restore.restoreDBPath()
	if err != nil {
		t.Fatalf("Failed to run .restoreDBPath(): %v", err.Error())
	} else if _, err := os.Stat(filepath.Join(restoreTmpPath, "storage.bson")); os.IsNotExist(err) {
		t.Fatal("Restored dbpath does not contain storage.bson!")
	}

	// start a wiredTiger test server using the restore data path
	var server dbtest.DBServer
	server.SetPath(tmpDBPath)
	server.SetEngine("wiredTiger")
	defer server.Stop()

	// get a test session
	session := server.Session()
	defer session.Close()

	// check the test hotbackup contains the doc in 'test.test': { _id "hotbackup", msg: "this should restore" }
	err = session.DB(testDB).C(testColl).Find(bson.M{"_id": "hotbackup", "msg": "this should restore"}).One(nil)
	if err != nil {
		t.Fatalf("Cannot find test doc in restored collection '%s.%s': %v", testDB, testColl, err.Error())
	}
}

func TestHotBackupRestoreClose(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", t.Name())
	if err != nil {
		t.Fatalf("Could not create tmpfile for lock test: %v", err.Error())
	}
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	restore := &Restore{lockFile: tmpfile.Name()}
	err = restore.getLock()
	if err != nil {
		t.Fatalf("Could not lock tmpfile: %v", err.Error())
	} else if !restore.lock.Locked() {
		t.Fatal(".getLock() did not lock the lockfile")
	}

	restore.Close()

	if restore.lock.Locked() {
		t.Fatal(".Close() did not unlock the lockfile")
	}
}
