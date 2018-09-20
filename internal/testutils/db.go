package testutils

import (
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/db"
)

func dialInfo(addrs []string, rs string) (*mgo.DialInfo, error) {
	di, err := db.NewDialInfo(&db.Config{
		Addrs:    addrs,
		Replset:  rs,
		Username: MongoDBUser,
		Password: MongoDBPassword,
		CertFile: MongoDBSSLPEMKeyFile,
		CAFile:   MongoDBSSLCACertFile,
		Timeout:  MongoDBTimeout,
	})
	if err != nil {
		return nil, err
	}
	return di, nil
}

func DialInfoForPort(t *testing.T, rs, port string) *mgo.DialInfo {
	di, err := dialInfo([]string{
		MongoDBHost + ":" + port},
		rs,
	)
	if err != nil {
		t.Fatalf(".DialInfoForPort() failed: %v", err.Error())
	}
	return di
}

func PrimaryDialInfo(t *testing.T, rs string) *mgo.DialInfo {
	di, err := dialInfo([]string{
		GetMongoDBAddr(rs, "primary")},
		rs,
	)
	if err != nil {
		t.Fatalf(".PrimaryDialInfo() failed: %v", err.Error())
	}
	return di
}

func ReplsetDialInfo(t *testing.T, rs string) *mgo.DialInfo {
	di, err := dialInfo(
		GetMongoDBReplsetAddrs(rs),
		rs,
	)
	if err != nil {
		t.Fatalf(".ReplsetDialInfo() failed: %v", err.Error())
	}
	di.Direct = false
	return di
}

func ConfigsvrReplsetDialInfo(t *testing.T) *mgo.DialInfo {
	di, err := dialInfo(
		GetMongoDBReplsetAddrs(MongoDBConfigsvrReplsetName),
		MongoDBConfigsvrReplsetName,
	)
	if err != nil {
		t.Fatalf(".ConfigsvrReplsetDialInfo() failed: %v", err.Error())
	}
	di.Direct = false
	return di
}

func MongosDialInfo(t *testing.T) *mgo.DialInfo {
	di, err := dialInfo([]string{MongoDBHost + ":" + MongoDBMongosPort}, "")
	if err != nil {
		t.Fatalf(".MongosDialInfo() failed: %v", err.Error())
	}
	return di
}
