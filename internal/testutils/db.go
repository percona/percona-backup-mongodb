package testutils

import (
	"os"
	"path/filepath"
	"time"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/db"
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
	MongoDBSSLDir               = filepath.Abs("../../docker/test/ssl")
	MongoDBSSLPEMKeyFile        = filepath.Join(MongoDBSSLDir, "client.pem")
	MongoDBSSLCACertFile        = filepath.Join(MongoDBSSLDir, "rootCA.crt")
	defaultAddr                 = []string{MongoDBHost + ":19001"}
)

func dialInfo(addrs []string) (*mgo.DialInfo, error) {
	di, err := db.NewDialInfo(&db.Config{
		Host:     addrs,
		Username: MongoDBUser,
		Password: MongoDBPassword,
		CertFile: MongoDBSSLPEMKeyFile,
		CAFile:   MongoDBSSLCACertFile,
		Timeout:  MongoDBTimeout,
	})
	if err != nil {
		return nil, err
	}
	if MongoDBReplsetName != "" {
		di.ReplicaSetName = MongoDBReplsetName
	}
	return di, nil
}

func DialInfoForPort(t *testing.T, port string) *mgo.DialInfo {
	di, err := dialInfo([]string{
		MongoDBHost + ":" + port,
	})
	if err != nil {
		t.Fatalf(".DialInfoForPort() failed: %v", err.Error())
	}
	return di
}

func PrimaryDialInfo(t *testing.T) *mgo.DialInfo {
	addrs := defaultAddr
	if MongoDBPrimaryPort != "" {
		addrs = []string{MongoDBHost + ":" + MongoDBPrimaryPort}
	}
	di, err := dialInfo(addrs)
	if err != nil {
		t.Fatalf(".PrimaryDialInfo() failed: %v", err.Error())
	}
	return di
}

func ReplsetDialInfo(t *testing.T) *mgo.DialInfo {
	var err error
	var di *mgo.DialInfo
	if MongoDBSecondary1Port != "" && MongoDBSecondary2Port != "" {
		di, err = PrimaryDialInfo()
	} else {
		di, err = dialInfo([]string{
			MongoDBHost + ":" + MongoDBPrimaryPort,
			MongoDBHost + ":" + MongoDBSecondary1Port,
			MongoDBHost + ":" + MongoDBSecondary2Port,
		})
	}
	if err != nil {
		t.Fatalf(".ReplsetDialInfo() failed: %v", err.Error())
	}
	di.Direct = false
	return di
}

func ConfigsvrReplsetDialInfo(t *testing.T) *mgo.DialInfo {
	if MongoDBConfigsvrReplsetName == "" || MongoDBConfigsvr1Port == "" {
		return &mgo.DialInfo{}
	}
	di, err := dialInfo([]string{MongoDBHost + ":" + MongoDBConfigsvr1Port})
	if err != nil {
		t.Fatalf(".ConfigsvrReplsetDialInfo() failed: %v", err.Error())
	}
	di.ReplicaSetName = MongoDBConfigsvrReplsetName
	di.Direct = false
	return di
}

func MongosDialInfo(t *testing.T) *mgo.DialInfo {
	if MongoDBMongosPort == "" {
		return &mgo.DialInfo{}
	}
	di, err := dialInfo([]string{MongoDBHost + ":" + MongoDBMongosPort})
	if err != nil {
		t.Fatalf(".MongosDialInfo() failed: %v", err.Error())
	}
	di.ReplicaSetName = ""
	return di
}
