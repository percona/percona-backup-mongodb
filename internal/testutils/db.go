package testutils

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/globalsign/mgo"
)

const (
	envMongoDBReplsetName    = "TEST_MONGODB_RS"
	envMongoDBPrimaryPort    = "TEST_MONGODB_PRIMARY_PORT"
	envMongoDBSecondary1Port = "TEST_MONGODB_SECONDARY1_PORT"
	envMongoDBSecondary2Port = "TEST_MONGODB_SECONDARY2_PORT"
	envMongoDBAdminUser      = "TEST_MONGODB_USERNAME"
	envMongoDBAdminPassword  = "TEST_MONGODB_PASSWORD"
)

var (
	MongodbHost           = "127.0.0.1"
	MongodbReplsetName    = os.Getenv(envMongoDBReplsetName)
	MongodbPrimaryPort    = os.Getenv(envMongoDBPrimaryPort)
	MongodbSecondary1Port = os.Getenv(envMongoDBSecondary1Port)
	MongodbSecondary2Port = os.Getenv(envMongoDBSecondary2Port)
	MongodbAdminUser      = os.Getenv(envMongoDBAdminUser)
	MongodbAdminPassword  = os.Getenv(envMongoDBAdminPassword)
	MongodbTimeout        = time.Duration(10) * time.Second
	defaultAddress        = MongodbHost + ":17001"
)

func getDialInfo() *mgo.DialInfo {

	return &mgo.DialInfo{
		Addrs:          []string{address},
		Direct:         true,
		Timeout:        MongodbTimeout,
		Username:       MongodbAdminUser,
		Password:       MongodbAdminPassword,
		ReplicaSetName: MongodbReplsetName,
	}
}

// GetSession returns a *mgo.Session configured for testing against a MongoDB Primary
func GetSession() (*mgo.Session, error) {
	dialInfo := getDialInfo(MongodbHost, port)
	session, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		return nil, err
	}
	return session, err
}
