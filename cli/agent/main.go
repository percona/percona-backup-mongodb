package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/grpc/client"
	"github.com/percona/mongodb-backup/internal/cluster"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type cliOptios struct {
	app           *kingpin.Application
	dsn           *string
	serverAddress *string
	tls           *bool
	caFile        *string
}

type hostInfo struct {
	Hostname          string
	HostOsType        string
	HostSystemCPUArch string
	HostDatabases     int
	HostCollections   int
	DBPath            string

	ProcPath         string
	ProcUserName     string
	ProcCreateTime   time.Time
	ProcProcessCount int

	// Server Status
	ProcessName    string
	ReplicasetName string
	Version        string
	NodeType       string
}

func main() {
	opts, err := processCliArgs()
	if err != nil {
		log.Fatalf("Cannot parse command line arguments: %s", err)
	}

	grpcOpts := getgRPCOptions(opts)
	clientID := fmt.Sprintf("ABC%04d", rand.Int63n(10000))
	log.Printf("Using Client ID: %s", clientID)

	// Connect to the mongodb-backup gRPC server
	conn, err := grpc.Dial(*opts.serverAddress, grpcOpts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	// Connect to the MongoDB instance
	mdbSession, err := mgo.Dial(*opts.serverAddress)
	if err != nil {
		log.Fatalf("Cannot connect to the %s: %s", *opts.serverAddress, err)
	}

	// Get MongoDB instance node type
	nodeType, err := getNodeType(mdbSession)

	// Run the mongodb-backup agent
	run(conn, mdbSession, clientID, nodeType)

}

func run(conn *grpc.ClientConn, mdbSession *mgo.Session, clientID string, nodeType pb.NodeType) {
	messagesClient := pb.NewMessagesClient(conn)
	rpcClient, err := client.NewClient(clientID, nodeType, messagesClient)
	if err != nil {
		log.Fatalf("Cannot create the rpc client: %s", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go processMessages(rpcClient, wg)

	rpcClient.StartStreamIO()

	wg.Wait()
	rpcClient.StopStreamIO()
}

func processMessages(rpcClient *client.Client, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		inMsg := <-rpcClient.InMsgChan()
		if inMsg == nil {
			return
		}
		fmt.Printf("%+v\n", inMsg)
		switch inMsg.Type {
		case pb.ServerMessage_GET_STATUS:
			//default:
		}
	}
}

func processCliArgs() (*cliOptios, error) {
	app := kingpin.New("mongodb-backup-client", "MongoDB backup client")
	opts := &cliOptios{
		app:           app,
		dsn:           app.Flag("dsn", "MongoDB connection string").String(),
		serverAddress: app.Flag("server-address", "MongoDB backup server address").String(),
		tls:           app.Flag("tls", "Use TLS").Bool(),
		caFile:        app.Flag("ca-file", "CA file").String(),
	}

	_, err := app.Parse(os.Args[1:])
	if err != nil {
		return nil, err
	}

	return opts, nil
}

func getgRPCOptions(opts *cliOptios) []grpc.DialOption {
	var grpcOpts []grpc.DialOption
	if *opts.tls {
		creds, err := credentials.NewClientTLSFromFile(*opts.caFile, "")
		if err != nil {
			log.Fatalf("Failed to create TLS credentials %v", err)
		}
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(creds))
	} else {
		grpcOpts = append(grpcOpts, grpc.WithInsecure())
	}
	return grpcOpts
}

func getNodeType(session *mgo.Session) (pb.NodeType, error) {
	isMaster, err := cluster.NewIsMaster(session)
	if err != nil {
		return pb.NodeType_UNDEFINED, err
	}
	if isMaster.IsShardServer() {
		return pb.NodeType_MONGOD_SHARDSVR, nil
	}
	if isMaster.IsReplset() {
		return pb.NodeType_MONGOD_REPLSET, nil
	}
	if isMaster.IsConfigServer() {
		return pb.NodeType_MONGOD_CONFIGSVR, nil
	}
	if isMaster.IsMongos() {
		return pb.NodeType_MONGOS, nil
	}
	return pb.NodeType_MONGOD, nil
}
