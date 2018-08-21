package hotbackup

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/otiai10/copy"
	flock "github.com/theckman/go-flock"
)

var (
	RestoreStopServerRetries = 120
	RestoreStopServerWait    = 500 * time.Millisecond
)

type Restore struct {
	backupPath     string
	dbPath         string
	session        *mgo.Session
	serverArgv     []string
	serverShutdown bool
	moveBackup     bool
	restored       bool
}

func NewRestore(session *mgo.Session, backupPath, dbPath string) (*Restore, error) {
	return &Restore{
		backupPath: backupPath,
		dbPath:     dbPath,
		session:    session,
	}, nil
}

func (r *Restore) isServerRunning() (bool, error) {
	if r.session != nil && r.session.Ping() == nil {
		return true, nil
	}
	lockFile := filepath.Join(r.dbPath, "mongod.lock")
	fi, err := os.Create(lockFile)
	if err != nil {
		return false, err
	}
	defer fi.Close()
	return flock.NewFlock(lockFile).Locked(), nil
}

func (r *Restore) getServerCmdLine() ([]string, error) {
	resp := struct {
		Argv []string `bson:"argv"`
	}{}
	err := r.session.Run(bson.D{{"getCmdLineOpts", 1}}, &resp)
	return resp.Argv, err
}

func (r *Restore) waitForShutdown() error {
	var tries int
	for tries < RestoreStopServerRetries {
		running, err := r.isServerRunning()
		if err != nil {
			return err
		} else if !running {
			r.serverShutdown = true
			return nil
		}
		time.Sleep(RestoreStopServerWait)
		tries++
	}
	return errors.New("server did not stop")
}

func (r *Restore) stopServer() error {
	running, err := r.isServerRunning()
	if err != nil {
		return err
	} else if !running {
		return nil
	}

	// get running server command-line
	r.serverArgv, err = r.getServerCmdLine()
	if err != nil {
		return err
	}

	// shutdown the running server, expect 'EOF' error from mongo session
	err = r.session.Run(bson.D{{"shutdown", 1}}, nil)
	if err == nil {
		return errors.New("no EOF from server")
	} else if err.Error() != "EOF" {
		return err
	}
	r.session.Close()
	r.session = nil

	return r.waitForShutdown()
}

func (r *Restore) restoreDBPath() error {
	// move backup to dbpath if enabled
	// or copy if not
	var err error
	if r.moveBackup {
		err = os.Rename(r.backupPath, r.dbPath)
	} else {
		err = copy.Copy(r.backupPath, r.dbPath)
	}
	if err != nil {
		return err
	}
	r.restored = true
	return nil
}

func (r *Restore) startServer() error {
	mongod := exec.Command(r.serverArgv[0], r.serverArgv[:1]...)
	return mongod.Start()
}
