package testutils

import (
	"os"
	"time"

	"github.com/globalsign/mgo"
)

const (
	envMongoDBReplsetName          = "TEST_MONGODB_RS"
	envMongoDBPrimaryPort          = "TEST_MONGODB_PRIMARY_PORT"
	envMongoDBSecondary1Port       = "TEST_MONGODB_SECONDARY1_PORT"
	envMongoDBSecondary2Port       = "TEST_MONGODB_SECONDARY2_PORT"
	envMongoDBConfigsvrReplsetName = "TEST_MONGODB_CONFIGSVR_RS"
	envMongoDBConfigsvr1Port       = "TEST_MONGODB_CONFIGSVR1_PORT"
	envMongoDBMongosPort           = "TEST_MONGODB_MONGOS_PORT"
	envMongoDBUser                 = "TEST_MONGODB_USERNAME"
	envMongoDBPassword             = "TEST_MONGODB_PASSWORD"
)

var (
	MongoDBHost                 = "127.0.0.1"
	MongoDBReplsetName          = os.Getenv(envMongoDBReplsetName)
	MongoDBPrimaryPort          = os.Getenv(envMongoDBPrimaryPort)
	MongoDBSecondary1Port       = os.Getenv(envMongoDBSecondary1Port)
	MongoDBSecondary2Port       = os.Getenv(envMongoDBSecondary2Port)
	MongoDBConfigsvrReplsetName = os.Getenv(envMongoDBConfigsvrReplsetName)
	MongoDBConfigsvr1Port       = os.Getenv(envMongoDBConfigsvr1Port)
	MongoDBMongosPort           = os.Getenv(envMongoDBMongosPort)
	MongoDBUser                 = os.Getenv(envMongoDBUser)
	MongoDBPassword             = os.Getenv(envMongoDBPassword)
	MongoDBTimeout              = time.Duration(10) * time.Second
	defaultAddr                 = []string{MongoDBHost + ":17001"}
)

func dialInfo(addrs []string) *mgo.DialInfo {
	di := &mgo.DialInfo{
		Addrs:   addrs,
		Timeout: MongoDBTimeout,
	}
	if MongoDBReplsetName != "" {
		di.ReplicaSetName = MongoDBReplsetName
	}
	if MongoDBUser != "" && MongoDBPassword != "" {
		di.Username = MongoDBUser
		di.Password = MongoDBPassword
	}
	return di
}

func PrimaryDialInfo() *mgo.DialInfo {
	addrs := defaultAddr
	if MongoDBPrimaryPort != "" {
		addrs = []string{MongoDBHost + ":" + MongoDBPrimaryPort}
	}
	di := dialInfo(addrs)
	di.Direct = true
	return di
}

func ReplsetDialInfo() *mgo.DialInfo {
	var di *mgo.DialInfo
	if MongoDBSecondary1Port != "" && MongoDBSecondary2Port != "" {
		di = PrimaryDialInfo()
	} else {
		di = dialInfo([]string{
			MongoDBHost + ":" + MongoDBPrimaryPort,
			MongoDBHost + ":" + MongoDBSecondary1Port,
			MongoDBHost + ":" + MongoDBSecondary2Port,
		})
	}
	di.Direct = false
	return di
}

func ConfigsvrReplsetDialInfo() *mgo.DialInfo {
	if MongoDBConfigsvrReplsetName == "" || MongoDBConfigsvr1Port == "" {
		return &mgo.DialInfo{}
	}
	di := dialInfo([]string{MongoDBHost + ":" + MongoDBConfigsvr1Port})
	di.ReplicaSetName = MongoDBConfigsvrReplsetName
	di.Direct = false
	return di
}

func MongosDialInfo() *mgo.DialInfo {
	if MongoDBMongosPort == "" {
		return &mgo.DialInfo{}
	}
	di := dialInfo([]string{MongoDBHost + ":" + MongoDBMongosPort})
	di.Direct = false
	return di
}
