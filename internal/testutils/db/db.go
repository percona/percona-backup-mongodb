package db

import (
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/internal/db"
	"github.com/percona/mongodb-backup/internal/testutils"
)

func dialInfo(addrs []string, rs string) (*mgo.DialInfo, error) {
	di, err := db.NewDialInfo(&db.Config{
		Addrs:    addrs,
		Replset:  rs,
		Username: testutils.MongoDBUser,
		Password: testutils.MongoDBPassword,
		CertFile: testutils.MongoDBSSLPEMKeyFile,
		CAFile:   testutils.MongoDBSSLCACertFile,
		Timeout:  testutils.MongoDBTimeout,
	})
	if err != nil {
		return nil, err
	}
	return di, nil
}

func DialInfoForPort(t *testing.T, rs, port string) *mgo.DialInfo {
	di, err := dialInfo([]string{
		testutils.MongoDBHost + ":" + port},
		rs,
	)
	if err != nil {
		t.Fatalf(".DialInfoForPort() failed: %v", err.Error())
	}
	return di
}

func PrimaryDialInfo(t *testing.T, rs string) *mgo.DialInfo {
	di, err := dialInfo([]string{
		testutils.GetMongoDBAddr(rs, "primary")},
		rs,
	)
	if err != nil {
		t.Fatalf(".PrimaryDialInfo() failed: %v", err.Error())
	}
	return di
}

func ReplsetDialInfo(t *testing.T, rs string) *mgo.DialInfo {
	di, err := dialInfo(
		testutils.GetMongoDBReplsetAddrs(rs),
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
		testutils.GetMongoDBReplsetAddrs(testutils.MongoDBConfigsvrReplsetName),
		testutils.MongoDBConfigsvrReplsetName,
	)
	if err != nil {
		t.Fatalf(".ConfigsvrReplsetDialInfo() failed: %v", err.Error())
	}
	di.Direct = false
	return di
}

func MongosDialInfo(t *testing.T) *mgo.DialInfo {
	di, err := dialInfo([]string{testutils.MongoDBHost + ":" + testutils.MongoDBMongosPort}, "")
	if err != nil {
		t.Fatalf(".MongosDialInfo() failed: %v", err.Error())
	}
	return di
}
