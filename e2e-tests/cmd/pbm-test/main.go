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

type bcpTyp string

const (
	bcpUnknown  bcpTyp = ""
	bcpLogical  bcpTyp = "logical"
	bcpPhysical bcpTyp = "physical"
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

	bcpT := bcpLogical
	if bcpTyp(os.Getenv("TESTS_BCP_TYPE")) == bcpPhysical {
		bcpT = bcpPhysical
	}
	log.Println("Backup Type:", bcpT, os.Getenv("TESTS_BCP_TYPE"))

	typ := testTyp(os.Getenv("TESTS_TYPE"))
	switch typ {
	case testsUnknown, testsSharded:
		runSharded(mUser, mPass, bcpT)
	case testsRS:
		runRS(mUser, mPass, bcpT)
	default:
		log.Fatalln("UNKNOWN TEST TYPE:", typ)
	}
}

func runRS(mUser, mPass string, bcpT bcpTyp) {
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

	switch bcpT {
	case bcpPhysical:
		runPhysical(tests, testsRS)
	default:
		run(tests, testsRS)
	}
}

func runSharded(mUser, mPass string, bcpT bcpTyp) {
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

	switch bcpT {
	case bcpPhysical:
		runPhysical(tests, testsSharded)
	default:
		run(tests, testsSharded)
	}
}
