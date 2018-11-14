package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"sort"
	"testing"

	"github.com/percona/mongodb-backup/internal/templates"
	"github.com/percona/mongodb-backup/internal/testutils"
	"github.com/percona/mongodb-backup/proto/api"
	pb "github.com/percona/mongodb-backup/proto/messages"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
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

	os.Exit(m.Run())
}

func TestListAgents(t *testing.T) {
	tmpDir := path.Join(os.TempDir(), "dump_test")
	defer os.RemoveAll(tmpDir) // Clean up after testing.
	l := log.New()
	d, err := testutils.NewGrpcDaemon(context.Background(), tmpDir, t, l)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}

	serverAddr := "127.0.0.1:" + testutils.TEST_GRPC_API_PORT
	conn, err := getApiConn(&cliOptions{ServerAddr: serverAddr})
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
			Id:             "127.0.0.1:17000",
			NodeType:       "MONGOS",
			NodeName:       "127.0.0.1:17000",
			ReplicasetName: "",
		},
		&api.Client{
			Version:        0,
			Id:             "127.0.0.1:17001",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetName: "rs1",
		},
		&api.Client{
			Version:        0,
			Id:             "127.0.0.1:17002",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetName: "rs1",
		},
		&api.Client{
			Version:        0,
			Id:             "127.0.0.1:17003",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetName: "rs1",
		},
		&api.Client{
			Version:        0,
			Id:             "127.0.0.1:17004",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetName: "rs2",
		},
		&api.Client{
			Version:        0,
			Id:             "127.0.0.1:17005",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetName: "rs2",
		},
		&api.Client{
			Version:        0,
			Id:             "127.0.0.1:17006",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetName: "rs2",
		},
		&api.Client{
			Version:        0,
			Id:             "127.0.0.1:17007",
			NodeType:       "MONGOD_CONFIGSVR",
			NodeName:       "127.0.0.1:17007",
			ReplicasetName: "csReplSet",
		},
	}

	for i, client := range clients {
		if client.Version != want[i].Version {
			t.Errorf("Invalid client version. Got %v, want %v", client.Version, want[i].Version)
		}
		if client.Id != want[i].Id {
			t.Errorf("Invalid client id. Got %v, want %v", client.Id, want[i].Id)
		}
		if client.NodeType != want[i].NodeType {
			t.Errorf("Invalid node type. Got %v, want %v", client.NodeType, want[i].NodeType)
		}
		if client.ReplicasetName != want[i].ReplicasetName {
			t.Errorf("Invalid replicaset name. Got %v, want %v", client.ReplicasetName, want[i].ReplicasetName)
		}
		if client.NodeType != "MONGOD_CONFIGSVR" && client.ClusterId == "" {
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

	if opts.ServerAddr == "" {
		return nil, fmt.Errorf("Invalid server address (nil or empty)")
	}

	if opts.TLS {
		if opts.CAFile == "" {
			opts.CAFile = testdata.Path("ca.pem")
		}
		creds, err := credentials.NewClientTLSFromFile(opts.CAFile, "")
		if err != nil {
			log.Fatalf("Failed to create TLS credentials %v", err)
		}
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(creds))
	} else {
		grpcOpts = append(grpcOpts, grpc.WithInsecure())
	}

	conn, err := grpc.Dial(opts.ServerAddr, grpcOpts...)
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
