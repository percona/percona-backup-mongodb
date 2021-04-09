package main

import (
	"log"
	"os"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/tests/sharded"
)

type testTyp string

const (
	testsUnknown testTyp = ""
	testsSharded testTyp = "sharded"
	testsRS      testTyp = "rs"
	tests1NodeRS testTyp = "node"

	defaultMongoUser = "bcp"
	defaultMongoPass = "test1234"

	dockerSocket = "unix:///var/run/docker.sock"
)

func main() {
	mUser := os.Getenv("BACKUP_USER")
	if mUser == "" {
		mUser = defaultMongoUser
	}
	mPass := os.Getenv("MONGO_PASS")
	if mPass == "" {
		mPass = defaultMongoPass
	}

	typ := testTyp(os.Getenv("TESTS_TYPE"))

	switch typ {
	case testsUnknown, testsSharded:
		runSharded(mUser, mPass)
	case testsRS:
		runRS(mUser, mPass)
	default:
		log.Fatalln("UNKNOWN TEST TYPE:", typ)
	}
}

func runRS(mUser, mPass string) {
	allTheNetworks := "mongodb://" + mUser + ":" + mPass + "@rs101:27017/"
	tests := sharded.New(sharded.ClusterConf{
		Mongos:          allTheNetworks,
		Configsrv:       allTheNetworks,
		ConfigsrvRsName: "rs1",
		Shards: map[string]string{
			"rs1": allTheNetworks,
		},
		DockerSocket: dockerSocket,
	})

	run(tests, testsRS)
}

func runSharded(mUser, mPass string) {
	tests := sharded.New(sharded.ClusterConf{
		Mongos:          "mongodb://" + mUser + ":" + mPass + "@mongos:27017/",
		Configsrv:       "mongodb://" + mUser + ":" + mPass + "@cfg01:27017/",
		ConfigsrvRsName: "cfg",
		Shards: map[string]string{
			"rs1": "mongodb://" + mUser + ":" + mPass + "@rs101:27017/",
			"rs2": "mongodb://" + mUser + ":" + mPass + "@rs201:27017/",
		},
		DockerSocket: dockerSocket,
	})

	run(tests, testsSharded)
}
