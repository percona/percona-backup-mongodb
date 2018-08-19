package hotbackup

import (
	"errors"
	"os"
	"strings"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/mdbstructs"
)

func isLocalhostSession(session *mgo.Session) (bool, error) {
	// get system hostname
	hostname, err := os.Hostname()
	if err != nil {
		return false, err
	}

	// check the server host == os.Hostname
	status := mdbstructs.ReplsetStatus{}
	err = session.Run(bson.D{{"serverStatus", "1"}}, &status)
	if err != nil {
		return false, err
	}
	split := strings.Split(status.Host, ":")
	if split[0] != hostname {
		return false, nil
	}

	// check connection is direct
	servers := session.LiveServers()
	if len(servers) != 1 {
		return false, errors.New("session is not direct")
	}
	split = strings.Split(servers[0], ":")
	for _, match := range []string{"127.0.0.1", "localhost", hostname} {
		if split[0] == match {
			return true, nil
		}
	}
	return false, nil
}

type HotBackup struct {
	backupDir string
	removed   bool
}

// New creates a Percona Server for MongoDB Hot Backup. The provided
// MongoDB session must be a direct connection to localhost/127.0.0.1
//
// https://www.percona.com/doc/percona-server-for-mongodb/LATEST/hot-backup.html
//
func New(session *mgo.Session, backupDir string) (*HotBackup, error) {
	isLocalhost, err := isLocalhostSession(session)
	if err != nil {
		return nil, err
	} else if !isLocalhost {
		return nil, errors.New("session must be direct session to localhost or 127.0.0.1")
	}
	hb := HotBackup{backupDir: backupDir}
	err = session.Run(bson.D{{"createBackup", 1}, {"backupDir", hb.backupDir}}, nil)
	if err != nil {
		return nil, err
	}
	return &hb, nil
}

// Dir returns the path to the Hot Backup directory
func (hb *HotBackup) Dir() string {
	return hb.backupDir
}

// Remove removes the Hot Backup directory and data
func (hb *HotBackup) Remove() error {
	if hb.removed {
		return nil
	}
	err := os.RemoveAll(hb.backupDir)
	if err != nil {
		return err
	}
	hb.backupDir = ""
	hb.removed = true
	return nil
}

// Close cleans-up and removes the Hot Backup
func (hb *HotBackup) Close() {
	_ = hb.Remove()
}
