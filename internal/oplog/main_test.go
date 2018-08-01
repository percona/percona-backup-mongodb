package oplog

import (
	"log"
	"os"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/testutils"
)

var (
	testSession *mgo.Session
)

func TestMain(m *testing.M) {
	var err error

	dialInfo := testutils.PrimaryDialInfo()
	testSession, err = mgo.DialWithInfo(dialInfo)
	if err != nil {
		log.Fatalf("Cannot connect to MongoDB: %s", err)
	}

	log.Printf("Connected to db, addrs: %v, auth: %v", testSession.LiveServers(), dialInfo.Password != "")

	exit := m.Run()

	log.Print("Closing connection to db")
	testSession.Close()

	os.Exit(exit)
}
