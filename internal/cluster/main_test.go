package cluster

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/globalsign/mgo"
)

var (
	EnvDBUri     = "TEST_MONGODB_URI"
	dbUri        = ""
	dbDefaultUri = "localhost:17001"
	testSession  *mgo.Session
)

func TestMain(m *testing.M) {
	var err error

	// get db uri from environment if it exists
	dbUri = strings.TrimSpace(os.Getenv(EnvDBUri))
	if dbUri != "" {
		fmt.Printf("Using mongodb uri from environment: %s\n", dbUri)
	} else {
		dbUri = dbDefaultUri
		fmt.Printf("Using default mongodb uri: %s\n", dbUri)
	}

	// get testSession
	testSession, err = mgo.DialWithInfo(&mgo.DialInfo{
		Addrs:    []string{dbUri},
		Timeout:  10 * time.Second,
		FailFast: true,
		Direct:   true,
	})
	if err != nil {
		fmt.Printf("Error creating test db session: %v\n", err)
		os.Exit(1)
	}

	exit := m.Run()
	testSession.Close()
	os.Exit(exit)
}
