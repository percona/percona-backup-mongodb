package main

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/kr/pretty"
	"github.com/percona/percona-backup-mongodb/grpc/client"
	yaml "gopkg.in/yaml.v2"
)

func TestDefaults(t *testing.T) {
	opts, err := processCliArgs([]string{"--backup-dir", os.TempDir()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		ServerAddress: defaultServerAddress,
		BackupDir:     os.TempDir(),
		MongodbConnOptions: client.ConnectionOptions{
			Host: defaultMongoDBHost,
			Port: defaultMongoDBPort,
		},
	}

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestOverrideDefaultsFromCommandLine(t *testing.T) {
	opts, err := processCliArgs([]string{"--backup-dir", os.TempDir(), "--server-address", "127.0.0.1:12345"})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		BackupDir:     os.TempDir(),
		ServerAddress: "127.0.0.1:12345",
		MongodbConnOptions: client.ConnectionOptions{
			Host: defaultMongoDBHost,
			Port: defaultMongoDBPort,
		},
	}

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestOverrideDefaultsFromEnv(t *testing.T) {
	os.Setenv("PBM_AGENT_SERVER_ADDRESS", "localhost:12345")
	opts, err := processCliArgs([]string{"--backup-dir", os.TempDir()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		ServerAddress: "localhost:12345",
		BackupDir:     os.TempDir(),
		MongodbConnOptions: client.ConnectionOptions{
			Host: defaultMongoDBHost,
			Port: defaultMongoDBPort,
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
		configFile:    tmpfile.Name(),
		BackupDir:     os.TempDir(),
		ServerAddress: "localhost:12345",
		MongodbConnOptions: client.ConnectionOptions{
			Host: defaultMongoDBHost,
			Port: defaultMongoDBPort,
		},
	}
	b, err := yaml.Marshal(wantOpts)

	if _, err := tmpfile.Write(b); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	opts, err := processCliArgs([]string{"--config-file", tmpfile.Name()})
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
		configFile:    tmpfile.Name(),
		BackupDir:     os.TempDir(),
		ServerAddress: "localhost:12345",
		MongodbConnOptions: client.ConnectionOptions{
			Host: defaultMongoDBHost,
			Port: defaultMongoDBPort,
		},
	}
	b, err := yaml.Marshal(wantOpts)

	if _, err := tmpfile.Write(b); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	os.Setenv("PBM_AGENT_BACKUP_DIR", "/some/dir")
	defer os.Setenv("PBM_AGENT_BACKUP_DIR", "")

	opts, err := processCliArgs([]string{"--config-file", tmpfile.Name()})
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

	opts, err := processCliArgs([]string{"--backup-dir", os.TempDir(), "--mongodb-port", "12345"})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		ServerAddress: "localhost:12345",
		BackupDir:     os.TempDir(),
		MongodbConnOptions: client.ConnectionOptions{
			Host: defaultMongoDBHost,
			Port: "12345",
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
		ServerAddress: "localhost:12345",
		BackupDir:     os.TempDir(),
		MongodbConnOptions: client.ConnectionOptions{
			Host: defaultMongoDBHost,
			Port: "12346",
		},
	}
	b, err := yaml.Marshal(wantOpts)

	if _, err := tmpfile.Write(b); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	wantOpts.MongodbConnOptions.Port = "12345"

	opts, err := processCliArgs([]string{"--backup-dir", os.TempDir(), "--config-file", tmpfile.Name(), "--mongodb-port", "12345"})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	opts.configFile = ""

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}
