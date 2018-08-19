package hotbackup

import (
	"errors"
	"strings"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/percona/mongodb-backup/mdbstructs"
)

type HotBackup struct {
	backupDir string
	response  *mdbstructs.OkResponse
	removed   bool
}

func isLocalhostSession(session *mgo.Session) bool {
	servers := session.LiveServers()
	if len(servers) != 1 {
		return false
	}
	split := strings.Split(servers[0], ":")
	return split[0] == "127.0.0.1" || split[0] == "localhost"
}

func New(session *mgo.Session, backupDir string) (*HotBackup, error) {
	if !isLocalhostSession(session) {
		return nil, errors.New("session must be direct session to localhost or 127.0.0.1")
	}
	hb := HotBackup{}
	err := session.Run(bson.D{{"createBackup", 1}, {"backupDir", backupDir}}, &hb.response)
	if err != nil {
		return nil, err
	}
	return &hb, nil
}

func (hb *HotBackup) Response() *mdbstructs.OkResponse {
	return hb.response
}

func (hb *HotBackup) Dir() string {
	return hb.backupDir
}

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

func (hb *HotBackup) Close() {
	_ = hb.Remove()
}
