package main

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/kr/pretty"
	"github.com/percona/percona-backup-mongodb/internal/utils"
	yaml "gopkg.in/yaml.v2"
)

func TestDefaults(t *testing.T) {
	opts, err := processCliParams([]string{})
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		GrpcPort:             defaultGrpcPort,
		APIPort:              defaultAPIPort,
		EnableClientsLogging: defaultClientsLogging,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		WorkDir:              utils.Expand(defaultWorkDir),
	}

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestOverrideDefaultsFromCommandLine(t *testing.T) {
	opts, err := processCliParams([]string{"--work-dir", os.TempDir(), "--api-port", "12345"})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		GrpcPort:             defaultGrpcPort,
		APIPort:              12345,
		EnableClientsLogging: defaultClientsLogging,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		WorkDir:              os.TempDir(),
	}

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestOverrideDefaultsFromEnv(t *testing.T) {
	os.Setenv("PBM_COORDINATOR_API_PORT", "12346")
	opts, err := processCliParams([]string{"--work-dir", os.TempDir()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		GrpcPort:             defaultGrpcPort,
		APIPort:              12346,
		EnableClientsLogging: defaultClientsLogging,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		WorkDir:              os.TempDir(),
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
		configFile:           tmpfile.Name(),
		GrpcPort:             98765,
		APIPort:              12346,
		EnableClientsLogging: defaultClientsLogging,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		WorkDir:              os.TempDir(),
	}
	b, err := yaml.Marshal(wantOpts)

	if _, err := tmpfile.Write(b); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	opts, err := processCliParams([]string{"--config-file", tmpfile.Name()})
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
		configFile:           tmpfile.Name(),
		GrpcPort:             98765,
		APIPort:              12346,
		EnableClientsLogging: defaultClientsLogging,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		WorkDir:              os.TempDir(),
	}
	b, err := yaml.Marshal(wantOpts)

	if _, err := tmpfile.Write(b); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	os.Setenv("PBM_COORDINATOR_API_PORT", "12347")

	opts, err := processCliParams([]string{"--config-file", tmpfile.Name()})
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
	os.Setenv("PBM_COORDINATOR_API_PORT", "12346")
	opts, err := processCliParams([]string{"--work-dir", os.TempDir(), "--api-port", "12345"})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	wantOpts := &cliOptions{
		GrpcPort:             defaultGrpcPort,
		APIPort:              12345, // command line args overrides env var value
		EnableClientsLogging: defaultClientsLogging,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		WorkDir:              os.TempDir(),
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
		GrpcPort:             98765,
		APIPort:              12346,
		EnableClientsLogging: defaultClientsLogging,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		WorkDir:              os.TempDir(),
	}
	b, err := yaml.Marshal(wantOpts)

	if _, err := tmpfile.Write(b); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	wantOpts.APIPort = 12345
	opts, err := processCliParams([]string{"--config-file", tmpfile.Name(), "--work-dir", os.TempDir(), "--api-port", "12345"})
	opts.configFile = "" // It is not exported so, it doesn't exists in the config file
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	opts.app = nil
	opts.configFile = "" // it is not exported so it doesn't exist in the config file

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}
