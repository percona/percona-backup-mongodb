package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"sort"
	"testing"

	"github.com/kr/pretty"
	"github.com/percona/percona-backup-mongodb/internal/templates"
	testGrpc "github.com/percona/percona-backup-mongodb/internal/testutils/grpc"
	"github.com/percona/percona-backup-mongodb/proto/api"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
	"gopkg.in/v1/yaml"
)

var (
	port    string
	apiPort string
)

func TestMain(m *testing.M) {
	log.SetLevel(log.ErrorLevel)
	if os.Getenv("DEBUG") == "1" {
		log.SetLevel(log.DebugLevel)
	}

	if port = os.Getenv("GRPC_PORT"); port == "" {
		port = "10000"
	}
	if apiPort = os.Getenv("GRPC_API_PORT"); apiPort == "" {
		apiPort = "10001"
	}
	// Override the default usage writer (io.Stdout) to not to show the errors during the tests
	usageWriter = &bytes.Buffer{}

	os.Exit(m.Run())
}

func TestListAgents(t *testing.T) {
	tmpDir := path.Join(os.TempDir(), "dump_test")
	defer os.RemoveAll(tmpDir) // Clean up after testing.
	l := log.New()
	d, err := testGrpc.NewGrpcDaemon(context.Background(), tmpDir, t, l)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}

	serverAddr := "127.0.0.1:" + testGrpc.TEST_GRPC_API_PORT
	conn, err := getApiConn(&cliOptions{ServerAddress: serverAddr})
	if err != nil {
		t.Fatalf("Cannot connect to the API: %s", err)
	}

	clients, err := connectedAgents(context.Background(), conn)
	if err != nil {
		t.Errorf("Cannot get connected clients list")
	}

	sort.Slice(clients, func(i, j int) bool { return clients[i].Id < clients[j].Id })
	want := []*api.Client{
		&api.Client{
			Version:        0,
			Id:             "127.0.0.1:17001",
			NodeType:       "NODE_TYPE_MONGOD_SHARDSVR",
			ReplicasetName: "rs1",
		},
		&api.Client{
			Version:        0,
			Id:             "127.0.0.1:17002",
			NodeType:       "NODE_TYPE_MONGOD_SHARDSVR",
			ReplicasetName: "rs1",
		},
		&api.Client{
			Version:        0,
			Id:             "127.0.0.1:17003",
			NodeType:       "NODE_TYPE_MONGOD_SHARDSVR",
			ReplicasetName: "rs1",
		},
		&api.Client{
			Version:        0,
			Id:             "127.0.0.1:17004",
			NodeType:       "NODE_TYPE_MONGOD_SHARDSVR",
			ReplicasetName: "rs2",
		},
		&api.Client{
			Version:        0,
			Id:             "127.0.0.1:17005",
			NodeType:       "NODE_TYPE_MONGOD_SHARDSVR",
			ReplicasetName: "rs2",
		},
		&api.Client{
			Version:        0,
			Id:             "127.0.0.1:17006",
			NodeType:       "NODE_TYPE_MONGOD_SHARDSVR",
			ReplicasetName: "rs2",
		},
		&api.Client{
			Version:        0,
			Id:             "127.0.0.1:17007",
			NodeType:       "NODE_TYPE_MONGOD_CONFIGSVR",
			NodeName:       "127.0.0.1:17007",
			ReplicasetName: "csReplSet",
		},
		&api.Client{
			Version:        0,
			Id:             "127.0.0.1:17000",
			NodeType:       "NODE_TYPE_MONGOS",
			NodeName:       "127.0.0.1:17000",
			ReplicasetName: "",
		},
	}

	for i, client := range clients {
		if client.Version != want[i].Version {
			t.Errorf("Invalid client version. Got %v, want %v", client.Version, want[i].Version)
		}
		// MongoS will return the hostname, but the hostname is different in different testing envs
		if client.NodeType != "NODE_TYPE_MONGOS" && client.Id != want[i].Id {
			t.Errorf("Invalid client id. Got %v, want %v", client.Id, want[i].Id)
		}
		if client.NodeType != want[i].NodeType {
			t.Errorf("Invalid node type. Got %v, want %v", client.NodeType, want[i].NodeType)
		}
		if client.ReplicasetName != want[i].ReplicasetName {
			t.Errorf("Invalid replicaset name. Got %v, want %v", client.ReplicasetName, want[i].ReplicasetName)
		}
		if client.NodeType != "NODE_TYPE_MONGOD_CONFIGSVR" && client.ClusterId == "" {
			t.Errorf("Invalid cluster ID (empty)")
		}
	}
	d.Stop()
}

func TestListAgentsVerbose(t *testing.T) {
	t.Skip("Templates test is used only for development")
	printTemplate(templates.ConnectedNodesVerbose, getTestClients())
}

func TestListAvailableBackups(t *testing.T) {
	t.Skip("Templates test is used only for development")
	b := map[string]*pb.BackupMetadata{
		"testfile1": &pb.BackupMetadata{Description: "description 1"},
		"testfile2": &pb.BackupMetadata{Description: "a long description 2 blah blah blah blah and blah"},
	}
	printTemplate(templates.AvailableBackups, b)
}

func getApiConn(opts *cliOptions) (*grpc.ClientConn, error) {
	var grpcOpts []grpc.DialOption

	if opts.ServerAddress == "" {
		return nil, fmt.Errorf("Invalid server address (nil or empty)")
	}

	if opts.TLS {
		if opts.TLSCAFile == "" {
			opts.TLSCAFile = testdata.Path("ca.pem")
		}
		creds, err := credentials.NewClientTLSFromFile(opts.TLSCAFile, "")
		if err != nil {
			log.Fatalf("Failed to create TLS credentials %v", err)
		}
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(creds))
	} else {
		grpcOpts = append(grpcOpts, grpc.WithInsecure())
	}

	conn, err := grpc.Dial(opts.ServerAddress, grpcOpts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	return conn, err
}

func getTestClients() []*api.Client {
	clients := []*api.Client{
		&api.Client{
			Version:        0,
			Id:             "6c8ba7b4-680f-42da-9798-97d8fd75e9ba",
			ClusterId:      "cid1",
			NodeType:       "MONGOS",
			NodeName:       "127.0.0.1:17000",
			ReplicasetName: "",
		},
		&api.Client{
			Version:        0,
			Id:             "701e4a24-3b9d-47d5-b3f2-0643a12f9462",
			ClusterId:      "cid1",
			NodeName:       "127.0.0.1:17001",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetId:   "rsid1",
			ReplicasetName: "rs1",
		},
		&api.Client{
			Version:        0,
			Id:             "8292319d-dd85-45a5-a671-59cb9ff72eca",
			ClusterId:      "cid1",
			NodeName:       "127.0.0.1:17002",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetId:   "rsid1",
			ReplicasetName: "rs1",
		},
		&api.Client{
			Version:        0,
			Id:             "8292319d-dd85-45a5-a671-59cb9ff72eca",
			ClusterId:      "cid1",
			NodeName:       "127.0.0.1:17003",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetId:   "rsid1",
			ReplicasetName: "rs1",
		},
		&api.Client{
			Version:        0,
			Id:             "8292319d-dd85-45a5-a671-59cb9ff72eca",
			ClusterId:      "cid1",
			NodeName:       "127.0.0.1:17004",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetId:   "rsid1",
			ReplicasetName: "rs2",
		},
		&api.Client{
			Version:        0,
			Id:             "8292319d-dd85-45a5-a671-59cb9ff72eca",
			ClusterId:      "cid1",
			NodeName:       "127.0.0.1:17005",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetId:   "rsid2",
			ReplicasetName: "rs2",
		},
		&api.Client{
			Version:        0,
			Id:             "8292319d-dd85-45a5-a671-59cb9ff72eca",
			ClusterId:      "cid1",
			NodeName:       "127.0.0.1:17006",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetId:   "rsid2",
			ReplicasetName: "rs2",
		},
		&api.Client{
			Version:        0,
			Id:             "7eb32bab-8f0a-4616-ab39-917d45591b7d",
			ClusterId:      "cid1",
			NodeType:       "MONGOD_CONFIGSVR",
			NodeName:       "127.0.0.1:17007",
			ReplicasetId:   "rsid2",
			ReplicasetName: "csReplSet",
		},
	}
	return clients
}

func TestDefaults(t *testing.T) {
	_, opts, err := processCliArgs([]string{})
	if err == nil {
		t.Errorf("Error expected, got nil")
	}
	if opts != nil {
		t.Errorf("opts should be nil, got: %+v", opts)
	}
}

func TestOverrideDefaultsFromCommandLine(t *testing.T) {
	cmd, opts, err := processCliArgs([]string{"run", "backup", "--description", "'some description'"})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	nilOpts(opts)

	wantOpts := &cliOptions{
		ServerAddress:    defaultServerAddress,
		ServerCompressor: defaultServerCompressor,
		TLS:              defaultTlsEnabled,
		TLSCAFile:        "",
		backupType:       defaultBackupType,
		description:      "'some description'",
		destinationType:  defaultDestinationType,
	}

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}

	if cmd != "run backup" {
		t.Errorf("Wanted command: 'run backup', got: %q", cmd)
	}
}

func TestOverrideDefaultsFromEnv(t *testing.T) {
	os.Setenv("PBMCTL_TLS", "true")
	defer os.Setenv("PBMCTL_TLS", "")

	cmd, opts, err := processCliArgs([]string{"list", "nodes"})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	nilOpts(opts)
	wantOpts := &cliOptions{
		TLS:              true,
		TLSCAFile:        "",
		ServerAddress:    defaultServerAddress,
		ServerCompressor: defaultServerCompressor,
		// Since the command is 'list nodes', this flags should be empty
		// destinationType:  defaultDestinationType,
		// backupType:       defaultBackupType,
	}

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}

	if cmd != "list nodes" {
		t.Errorf("Invalid command. Want 'list nodes', got %q", cmd)
	}
}

func TestOverrideDefaultsFromConfigFile(t *testing.T) {
	tmpfile, err := ioutil.TempFile("", "test")
	if err != nil {
		t.Fatalf("cannot create temp config file: %s", err)
	}

	defer os.Remove(tmpfile.Name())
	wantOpts := &cliOptions{
		TLS:              true,
		TLSCAFile:        "/some/fake/file",
		ServerAddress:    defaultServerAddress,
		ServerCompressor: "",
	}
	b, err := yaml.Marshal(wantOpts)

	if _, err := tmpfile.Write(b); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	_, opts, err := processCliArgs([]string{"list", "nodes", "--config-file", tmpfile.Name()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	nilOpts(opts)
	wantOpts.configFile = tmpfile.Name()

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
		TLS:              false,
		TLSCAFile:        "/another/fake/file",
		ServerAddress:    defaultServerAddress,
		ServerCompressor: "",
	}
	b, err := yaml.Marshal(wantOpts)

	if _, err := tmpfile.Write(b); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	os.Setenv("PBMCTL_CA_FILE", "/a/fake/file")
	defer os.Setenv("PBMCTL_CA_FILE", "")

	_, opts, err := processCliArgs([]string{"list", "nodes", "--config-file", tmpfile.Name()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	nilOpts(opts)
	wantOpts.configFile = tmpfile.Name()

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func TestCommandLineArgsPrecedenceOverEnvVars(t *testing.T) {
	// env var name should be: app name _ param name (see Kingpin doc for DefaultEnvars())
	os.Setenv("PBMCTL_CA_FILE", "/a/fake/file")
	defer os.Setenv("PBMCTL_CA_FILE", "")
	tmpfile, err := ioutil.TempFile("", "test")
	if err != nil {
		t.Fatalf("cannot create temp config file: %s", err)
	}

	defer os.Remove(tmpfile.Name())

	_, opts, err := processCliArgs([]string{"list", "nodes", "--tls-ca-file", tmpfile.Name()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	nilOpts(opts)
	wantOpts := &cliOptions{
		TLS:              false,
		TLSCAFile:        tmpfile.Name(),
		ServerAddress:    defaultServerAddress,
		ServerCompressor: defaultServerCompressor,
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

	fakeCAFile, err := ioutil.TempFile("", "test")
	if err != nil {
		t.Fatalf("cannot create fake CA file: %s", err)
	}
	defer os.Remove(fakeCAFile.Name())

	wantOpts := &cliOptions{
		TLS:              false,
		TLSCAFile:        "/a/fake/ca/file",
		ServerAddress:    defaultServerAddress,
		ServerCompressor: defaultServerCompressor,
	}
	b, err := yaml.Marshal(wantOpts)

	if _, err := tmpfile.Write(b); err != nil {
		log.Fatal(err)
	}
	if err := tmpfile.Close(); err != nil {
		log.Fatal(err)
	}

	_, opts, err := processCliArgs([]string{"list", "nodes", "--tls-ca-file", fakeCAFile.Name()})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	nilOpts(opts)
	wantOpts.TLSCAFile = fakeCAFile.Name()

	if !reflect.DeepEqual(opts, wantOpts) {
		t.Errorf("Invalid default options. Want: \n%s\nGot:\n%s", pretty.Sprint(wantOpts), pretty.Sprint(opts))
	}
}

func nilOpts(opts *cliOptions) {
	opts.app = nil
	opts.backup = nil
	opts.restore = nil
	opts.list = nil
	opts.listNodes = nil
	opts.listBackups = nil
}
