package testutils

import (
	"os"
	"time"

	"github.com/globalsign/mgo"
)

const (
	envMongoDBReplsetName    = "TEST_MONGODB_RS"
	envMongoDBPrimaryPort    = "TEST_MONGODB_PRIMARY_PORT"
	envMongoDBSecondary1Port = "TEST_MONGODB_SECONDARY1_PORT"
	envMongoDBSecondary2Port = "TEST_MONGODB_SECONDARY2_PORT"
	envMongoDBUser           = "TEST_MONGODB_USERNAME"
	envMongoDBPassword       = "TEST_MONGODB_PASSWORD"
)

var (
	MongodbHost           = "127.0.0.1"
	MongodbReplsetName    = os.Getenv(envMongoDBReplsetName)
	MongodbPrimaryPort    = os.Getenv(envMongoDBPrimaryPort)
	MongodbSecondary1Port = os.Getenv(envMongoDBSecondary1Port)
	MongodbSecondary2Port = os.Getenv(envMongoDBSecondary2Port)
	MongodbUser           = os.Getenv(envMongoDBUser)
	MongodbPassword       = os.Getenv(envMongoDBPassword)
	MongodbTimeout        = time.Duration(10) * time.Second
	defaultAddr           = []string{MongodbHost + ":17001"}
)

func dialInfo(addrs []string) *mgo.DialInfo {
	di := &mgo.DialInfo{
		Addrs:   addrs,
		Timeout: MongodbTimeout,
	}
	if MongodbReplsetName != "" {
		di.ReplicaSetName = MongodbReplsetName
	}
	if MongodbUser != "" && MongodbPassword != "" {
		di.Username = MongodbUser
		di.Password = MongodbPassword
	}
	return di
}

func PrimaryDialInfo() *mgo.DialInfo {
	addrs := defaultAddr
	if MongodbPrimaryPort != "" {
		addrs = []string{MongodbHost + ":" + MongodbPrimaryPort}
	}
	di := dialInfo(addrs)
	di.Direct = true
	return di
}

func ReplsetDialInfo() *mgo.DialInfo {
	var di *mgo.DialInfo
	if MongodbSecondary1Port != "" && MongodbSecondary2Port != "" {
		di = PrimaryDialInfo()
	} else {
		di = dialInfo([]string{
			MongodbHost + ":" + MongodbPrimaryPort,
			MongodbHost + ":" + MongodbSecondary1Port,
			MongodbHost + ":" + MongodbSecondary2Port,
		})
	}
	di.Direct = false
	return di
}
