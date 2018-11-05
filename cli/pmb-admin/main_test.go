package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"

	testGrpc "github.com/percona/mongodb-backup/internal/testutils/grpc"
	"github.com/percona/mongodb-backup/proto/api"
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
	l := log.New()
	d, err := testGrpc.NewGrpcDaemon(context.Background(), t, l)
	if err != nil {
		t.Fatalf("cannot start a new gRPC daemon/clients group: %s", err)
	}

	serverAddr := "127.0.0.1:" + testGrpc.TEST_GRPC_API_PORT
	conn, err := getApiConn(&cliOptions{serverAddr: &serverAddr})
	if err != nil {
		t.Fatalf("Cannot connect to the API: %s", err)
	}

	clients, err := connectedAgents(conn)
	if err != nil {
		t.Errorf("Cannot get connected clients list")
	}

	sort.Slice(clients, func(i, j int) bool { return clients[i].ID < clients[j].ID })
	want := []*api.Client{
		&api.Client{
			Version:        0,
			ID:             "127.0.0.1:17000",
			NodeType:       "MONGOS",
			NodeName:       "127.0.0.1:17000",
			ReplicasetName: "",
		},
		&api.Client{
			Version:        0,
			ID:             "127.0.0.1:17001",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetName: "rs1",
		},
		&api.Client{
			Version:        0,
			ID:             "127.0.0.1:17002",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetName: "rs1",
		},
		&api.Client{
			Version:        0,
			ID:             "127.0.0.1:17003",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetName: "rs1",
		},
		&api.Client{
			Version:        0,
			ID:             "127.0.0.1:17004",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetName: "rs2",
		},
		&api.Client{
			Version:        0,
			ID:             "127.0.0.1:17005",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetName: "rs2",
		},
		&api.Client{
			Version:        0,
			ID:             "127.0.0.1:17006",
			NodeType:       "MONGOD_SHARDSVR",
			ReplicasetName: "rs2",
		},
		&api.Client{
			Version:        0,
			ID:             "127.0.0.1:17007",
			NodeType:       "MONGOD_CONFIGSVR",
			NodeName:       "127.0.0.1:17007",
			ReplicasetName: "csReplSet",
		},
	}

	for i, client := range clients {
		if client.Version != want[i].Version {
			t.Errorf("Invalid client version. Got %v, want %v", client.Version, want[i].Version)
		}
		if client.ID != want[i].ID {
			t.Errorf("Invalid client id. Got %v, want %v", client.ID, want[i].ID)
		}
		if client.NodeType != want[i].NodeType {
			t.Errorf("Invalid node type. Got %v, want %v", client.NodeType, want[i].NodeType)
		}
		if client.ReplicasetName != want[i].ReplicasetName {
			t.Errorf("Invalid replicaset name. Got %v, want %v", client.ReplicasetName, want[i].ReplicasetName)
		}
		if client.NodeType != "MONGOD_CONFIGSVR" && client.ClusterID == "" {
			t.Errorf("Invalid cluster ID (empty)")
		}
	}
	d.Stop()
}

func getApiConn(opts *cliOptions) (*grpc.ClientConn, error) {
	var grpcOpts []grpc.DialOption

	if opts.serverAddr == nil || *opts.serverAddr == "" {
		return nil, fmt.Errorf("Invalid server address (nil or empty)")
	}

	if opts.tls != nil && *opts.tls {
		if opts.caFile != nil && *opts.caFile == "" {
			*opts.caFile = testdata.Path("ca.pem")
		}
		creds, err := credentials.NewClientTLSFromFile(*opts.caFile, "")
		if err != nil {
			log.Fatalf("Failed to create TLS credentials %v", err)
		}
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(creds))
	} else {
		grpcOpts = append(grpcOpts, grpc.WithInsecure())
	}

	conn, err := grpc.Dial(*opts.serverAddr, grpcOpts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	return conn, err
}
