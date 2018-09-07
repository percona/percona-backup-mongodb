package testutils

import (
	"os"
	"path/filepath"
	"time"
)

const (
	envMongoDBShard1ReplsetName    = "TEST_MONGODB_S1_RS"
	envMongoDBShard1PrimaryPort    = "TEST_MONGODB_S1_PRIMARY_PORT"
	envMongoDBShard1Secondary1Port = "TEST_MONGODB_S1_SECONDARY1_PORT"
	envMongoDBShard1Secondary2Port = "TEST_MONGODB_S1_SECONDARY2_PORT"
	envMongoDBShard2ReplsetName    = "TEST_MONGODB_S2_RS"
	envMongoDBShard2PrimaryPort    = "TEST_MONGODB_S2_PRIMARY_PORT"
	envMongoDBShard2Secondary1Port = "TEST_MONGODB_S2_SECONDARY1_PORT"
	envMongoDBShard2Secondary2Port = "TEST_MONGODB_S2_SECONDARY2_PORT"
	envMongoDBConfigsvrReplsetName = "TEST_MONGODB_CONFIGSVR_RS"
	envMongoDBConfigsvr1Port       = "TEST_MONGODB_CONFIGSVR1_PORT"
	envMongoDBMongosPort           = "TEST_MONGODB_MONGOS_PORT"
	envMongoDBUser                 = "TEST_MONGODB_USERNAME"
	envMongoDBPassword             = "TEST_MONGODB_PASSWORD"
)

var (
	MongoDBHost                 = "127.0.0.1"
	MongoDBShard1ReplsetName    = os.Getenv(envMongoDBShard1ReplsetName)
	MongoDBShard1PrimaryPort    = os.Getenv(envMongoDBShard1PrimaryPort)
	MongoDBShard1Secondary1Port = os.Getenv(envMongoDBShard1Secondary1Port)
	MongoDBShard1Secondary2Port = os.Getenv(envMongoDBShard1Secondary2Port)
	MongoDBShard2ReplsetName    = os.Getenv(envMongoDBShard2ReplsetName)
	MongoDBShard2PrimaryPort    = os.Getenv(envMongoDBShard2PrimaryPort)
	MongoDBShard2Secondary1Port = os.Getenv(envMongoDBShard2Secondary1Port)
	MongoDBShard2Secondary2Port = os.Getenv(envMongoDBShard2Secondary2Port)
	MongoDBConfigsvrReplsetName = os.Getenv(envMongoDBConfigsvrReplsetName)
	MongoDBConfigsvr1Port       = os.Getenv(envMongoDBConfigsvr1Port)
	MongoDBMongosPort           = os.Getenv(envMongoDBMongosPort)
	MongoDBUser                 = os.Getenv(envMongoDBUser)
	MongoDBPassword             = os.Getenv(envMongoDBPassword)
	MongoDBTimeout              = time.Duration(10) * time.Second
	MongoDBSSLDir               = "../../docker/test/ssl"
	MongoDBSSLPEMKeyFile        = filepath.Join(MongoDBSSLDir, "client.pem")
	MongoDBSSLCACertFile        = filepath.Join(MongoDBSSLDir, "rootCA.crt")

	// test mongodb hosts map
	hosts = map[string]map[string]string{
		MongoDBShard1ReplsetName: map[string]string{
			"primary":    MongoDBHost + ":" + MongoDBShard1PrimaryPort,
			"secondary1": MongoDBHost + ":" + MongoDBShard1Secondary1Port,
			"secondary2": MongoDBHost + ":" + MongoDBShard1Secondary2Port,
		},
		MongoDBShard2ReplsetName: map[string]string{
			"primary":    MongoDBHost + ":" + MongoDBShard2PrimaryPort,
			"secondary1": MongoDBHost + ":" + MongoDBShard2Secondary1Port,
			"secondary2": MongoDBHost + ":" + MongoDBShard2Secondary2Port,
		},
		MongoDBConfigsvrReplsetName: map[string]string{
			"primary": MongoDBHost + ":" + MongoDBConfigsvr1Port,
		},
	}
)

func GetMongoDBAddr(rs, name string) string {
	if _, ok := hosts[rs]; !ok {
		return ""
	}
	replset := hosts[rs]
	if host, ok := replset[name]; ok {
		return host
	}
	return ""
}

func GetMongoDBReplsetAddrs(rs string) []string {
	addrs := []string{}
	if _, ok := hosts[rs]; !ok {
		return addrs
	}
	for _, host := range hosts[rs] {
		addrs = append(addrs, host)
	}
	return addrs
}
