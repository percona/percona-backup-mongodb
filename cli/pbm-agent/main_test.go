package main

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/kr/pretty"
	"github.com/percona/percona-backup-mongodb/grpc/client"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
	yaml "gopkg.in/yaml.v2"
)

func TestNoStorages(t *testing.T) {
	_, err := processCliArgs([]string{})
	if err == nil {
		t.Fatalf("--storage-config should be mandatory")
	}
}

func TestDefaults(t *testing.T) {
	opts, err := processCliArgs([]string{"--storage-config", storagesFile()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		ServerAddress:  defaultServerAddress,
		StoragesConfig: storagesFile(),
		MongodbConnOptions: client.ConnectionOptions{
			Host:           defaultMongoDBHost,
			Port:           defaultMongoDBPort,
			ReconnectDelay: defaultReconnectDelay,
			ReconnectCount: defaultReconnectCount,
		},
	}

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestOverrideDefaultsFromCommandLine(t *testing.T) {
	opts, err := processCliArgs([]string{"--server-address", "127.0.0.1:12345", "--storage-config", storagesFile()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		ServerAddress:  "127.0.0.1:12345",
		StoragesConfig: storagesFile(),
		MongodbConnOptions: client.ConnectionOptions{
			Host:           defaultMongoDBHost,
			Port:           defaultMongoDBPort,
			ReconnectDelay: defaultReconnectDelay,
			ReconnectCount: defaultReconnectCount,
		},
	}

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestMongoDBSSL(t *testing.T) {
	opts := &cliOptions{
		MongodbConnOptions: client.ConnectionOptions{
			Host:           testutils.MongoDBHost,
			Port:           testutils.MongoDBShard1PrimaryPort,
			User:           testutils.MongoDBUser,
			Password:       testutils.MongoDBPassword,
			AuthDB:         "admin",
			ReplicasetName: testutils.MongoDBShard1ReplsetName,
		},
		MongodbSslOptions: client.SSLOptions{
			SSLCAFile:     path.Join(testutils.BaseDir(), "docker", "test", "ssl", "rootCA.crt"),
			SSLPEMKeyFile: path.Join(testutils.BaseDir(), "docker", "test", "ssl", "client.pem"),
		},
	}

	di, err := buildDialInfo(opts)
	if err != nil {
		t.Errorf("Cannot get dial info: %s", err)
	}

	sess, err := dbConnect(di, 1, 0)
	if err != nil {
		t.Errorf("Cannot connect to the db: %s", err)
	}
	sess.Close()
}

func TestOverrideDefaultsFromEnv(t *testing.T) {
	os.Setenv("PBM_AGENT_SERVER_ADDRESS", "localhost:12345")
	opts, err := processCliArgs([]string{"--storage-config", storagesFile()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		ServerAddress:  "localhost:12345",
		StoragesConfig: storagesFile(),
		MongodbConnOptions: client.ConnectionOptions{
			Host:           defaultMongoDBHost,
			Port:           defaultMongoDBPort,
			ReconnectDelay: defaultReconnectDelay,
			ReconnectCount: defaultReconnectCount,
		},
	}

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestOverrideDefaultsFromConfigFile(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "test")
	if err != nil {
		t.Fatalf("cannot create temp config file: %s", err)
	}

	defer os.Remove(tmpfile.Name())
	wantOpts := &cliOptions{
		configFile:     tmpfile.Name(),
		ServerAddress:  "localhost:12345",
		StoragesConfig: storagesFile(),
		MongodbConnOptions: client.ConnectionOptions{
			Host:           defaultMongoDBHost,
			Port:           defaultMongoDBPort,
			ReconnectDelay: defaultReconnectDelay,
			ReconnectCount: defaultReconnectCount,
		},
	}
	b, _ := yaml.Marshal(wantOpts)

	if _, err := tmpfile.Write(b); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	opts, err := processCliArgs([]string{"--config-file", tmpfile.Name(), "--storage-config", storagesFile()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestConfigfileEnvPrecedenceOverEnvVars(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "test")
	if err != nil {
		t.Fatalf("cannot create temp config file: %s", err)
	}

	defer os.Remove(tmpfile.Name())
	wantOpts := &cliOptions{
		configFile:     tmpfile.Name(),
		ServerAddress:  "localhost:12345",
		StoragesConfig: storagesFile(),
		MongodbConnOptions: client.ConnectionOptions{
			Host:           defaultMongoDBHost,
			Port:           defaultMongoDBPort,
			ReconnectDelay: defaultReconnectDelay,
			ReconnectCount: defaultReconnectCount,
		},
	}
	b, _ := yaml.Marshal(wantOpts)

	if _, err := tmpfile.Write(b); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	os.Setenv("PBM_AGENT_BACKUP_DIR", "/some/dir")
	defer os.Setenv("PBM_AGENT_BACKUP_DIR", "")

	opts, err := processCliArgs([]string{"--config-file", tmpfile.Name(), "--storage-config", storagesFile()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestCommandLineArgsPrecedenceOverEnvVars(t *testing.T) {
	// env var name should be: app name _ param name (see Kingpin doc for DefaultEnvars())
	os.Setenv("PBM_AGENT_MONGODB_PORT", "12346")
	defer os.Setenv("PBM_AGENT_MONGODB_PORT", "")

	opts, err := processCliArgs([]string{"--mongodb-port", "12345", "--storage-config", storagesFile()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		ServerAddress:  "localhost:12345",
		StoragesConfig: storagesFile(),
		MongodbConnOptions: client.ConnectionOptions{
			Host:           defaultMongoDBHost,
			Port:           "12345",
			ReconnectDelay: defaultReconnectDelay,
			ReconnectCount: defaultReconnectCount,
		},
	}

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestCommandLineArgsPrecedenceOverConfig(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "test")
	if err != nil {
		t.Fatalf("cannot create temp config file: %s", err)
	}

	defer os.Remove(tmpfile.Name())
	wantOpts := &cliOptions{
		ServerAddress:  "localhost:12345",
		StoragesConfig: storagesFile(),
		MongodbConnOptions: client.ConnectionOptions{
			Host:           defaultMongoDBHost,
			Port:           "12346",
			ReconnectDelay: defaultReconnectDelay,
			ReconnectCount: defaultReconnectCount,
		},
	}
	b, _ := yaml.Marshal(wantOpts)

	if _, err := tmpfile.Write(b); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	wantOpts.MongodbConnOptions.Port = "12345"

	opts, err := processCliArgs([]string{
		"--config-file", tmpfile.Name(),
		"--mongodb-port", "12345",
		"--storage-config", storagesFile(),
	})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	opts.configFile = ""

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func storagesFile() string {
	return filepath.Join(testutils.BaseDir(), "testdata", "storage.yml")
}
