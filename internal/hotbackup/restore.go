package hotbackup

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

type Restore struct {
	dbPath         string
	session        *mgo.Session
	serverArgv     []string
	serverShutdown bool
	restored       bool
}

func NewRestore(session *mgo.Session, dbPath string) (*Restore, error) {
	return &Restore{
		dbPath:  dbPath,
		session: session,
	}, nil
}

func (r *Restore) isServerRunning() bool {
	if r.session.Ping() == nil {
		return true
	}
	lockFile := filepath.Join(r.dbPath, "mongod.lock")
	if _, err := os.Stat(lockFile); os.IsNotExist(err) {
		return false
	}
	return true
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
	for tries < 60 {
		if !r.isServerRunning() {
			r.serverShutdown = true
			return nil
		}
		time.Sleep(time.Second)
		tries++
	}
	return errors.New("server did not stop")
}

func (r *Restore) stopServer() error {
	if !r.isServerRunning() {
		return nil
	}

	// get running server command-line
	var err error
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

	return r.waitForShutdown()
}

func (r *Restore) startServer() error {
	mongod := exec.Command(r.serverArgv[0], r.serverArgv[:1]...)
	return mongod.Start()
}
