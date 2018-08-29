package test_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"testing"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/grpc/api"
	"github.com/percona/mongodb-backup/grpc/client"
	"github.com/percona/mongodb-backup/grpc/server"
	"github.com/percona/mongodb-backup/internal/testutils"
	pbapi "github.com/percona/mongodb-backup/proto/api"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"google.golang.org/grpc"
)

var port, apiPort string

func TestMain(m *testing.M) {

	if port = os.Getenv("GRPC_PORT"); port == "" {
		port = "10000"
	}
	if apiPort = os.Getenv("GRPC_API_PORT"); apiPort == "" {
		apiPort = "10001"
	}

	os.Exit(m.Run())
}

func TestGlobal(t *testing.T) {
	var opts []grpc.ServerOption
	stopChan := make(chan interface{})
	wg := &sync.WaitGroup{}

	// Start the grpc server
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", port))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// This is the sever/agents gRPC server
	grpcServer := grpc.NewServer(opts...)
	messagesServer := server.NewMessagesServer()
	pb.RegisterMessagesServer(grpcServer, messagesServer)

	wg.Add(1)
	log.Printf("Starting agents gRPC server. Listening on %s", lis.Addr().String())
	runAgentsGRPCServer(grpcServer, lis, stopChan, wg)

	//
	apilis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", apiPort))
	if err != nil {
		t.Fatal(err)
	}

	// This is the server gRPC API
	apiGrpcServer := grpc.NewServer(opts...)
	apiServer := api.NewApiServer(messagesServer)
	pbapi.RegisterApiServer(apiGrpcServer, apiServer)

	wg.Add(1)
	log.Printf("Starting API gRPC server. Listening on %s", apilis.Addr().String())
	runAgentsGRPCServer(apiGrpcServer, apilis, stopChan, wg)

	// Let's start an agent
	clientOpts := []grpc.DialOption{grpc.WithInsecure()}

	clientServerAddr := fmt.Sprintf("127.0.0.1:%s", port)
	clientConn, err := grpc.Dial(clientServerAddr, clientOpts...)

	ports := []string{testutils.MongoDBPrimaryPort, testutils.MongoDBSecondary1Port, testutils.MongoDBSecondary2Port}

	clients := []*client.Client{}
	for i, port := range ports {
		di := testutils.DialInfoForPort(port)
		session, err := mgo.DialWithInfo(di)
		log.Printf("Connecting agent #%d to: %s\n", i, di.Addrs[0])
		if err != nil {
			t.Fatalf("Cannot connect to the MongoDB server %q: %s", di.Addrs[0], err)
		}
		session.SetMode(mgo.Eventual, true)

		agentID := fmt.Sprintf("PMB-%03d", i)

		client, err := client.NewClient(ctx, session, clientConn)
		if err != nil {
			t.Fatalf("Cannot create an agent instance %s: %s", agentID, err)
		}
		clients = append(clients, client)
	}

	clientsList := messagesServer.Clients()
	if len(clientsList) != 3 {
		t.Errorf("Want 3 connected clients, got %d", len(clientsList))
	}
	// Left here for debugging
	// if testing.Verbose() {
	// 	for key, client := range clientsList {
	// 		fmt.Printf("Key: %s ************************\n", key)
	// 		fmt.Printf("ID               : %s\n", client.ID)
	// 		fmt.Printf("Node Type        : %v\n", client.NodeType)
	// 		fmt.Printf("Node Name        : %v\n", client.NodeName)
	// 		fmt.Printf("Cluster ID       : %v\n", client.ClusterID)
	// 		fmt.Printf("Last command sent: %v\n", client.LastCommandSent)
	// 		fmt.Printf("Last seen        : %v\n", client.LastSeen)
	// 		fmt.Printf("Replicaset name  : %v\n", client.ReplicasetName)
	// 		fmt.Printf("Replicaset ID    : %v\n", client.ReplicasetID)
	// 		fmt.Printf("Status\n")
	// 		fmt.Printf("   ReplicaSetVersion : %v\n", client.Status.ReplicasetVersion)
	// 		fmt.Printf("   LastOplogTime     : %v\n", client.Status.LastOplogTs)
	// 		fmt.Printf("   RunningDBBackup   : %v\n", client.Status.DBBackUpRunning)
	// 	}
	// }

	clientsByReplicaset := messagesServer.ClientsByReplicaset()
	var firstClient *server.Client
	for _, client := range clientsByReplicaset {
		if len(client) == 0 {
			break
		}
		firstClient = client[0]
		break
	}

	status, err := firstClient.GetStatus()
	if err != nil {
		t.Errorf("Cannot get first client status: %s", err)
	}
	if status.BackupType != pb.BackupType_LOGICAL {
		t.Errorf("The default backup type should be 0 (Logical). Got backup type: %v", status.BackupType)
	}

	backupSource, err := firstClient.GetBackupSource()
	if err != nil {
		t.Errorf("Cannot get backup source: %s", err)
	}
	if backupSource == "" {
		t.Error("Received empty backup source")
	}
	log.Printf("Received backup source: %v", backupSource)

	for _, client := range clients {
		client.Stop()
	}

	cancel()
	messagesServer.Stop()
	close(stopChan)
	wg.Wait()
}

func runAgentsGRPCServer(grpcServer *grpc.Server, lis net.Listener, stopChan chan interface{}, wg *sync.WaitGroup) {
	go func() {
		err := grpcServer.Serve(lis)
		if err != nil {
			log.Printf("Cannot start agents gRPC server: %s", err)
		}
		log.Println("Stopping server " + lis.Addr().String())
		wg.Done()
	}()

	go func() {
		<-stopChan
		log.Printf("Gracefuly stopping server at %s", lis.Addr().String())
		grpcServer.GracefulStop()
	}()
}
