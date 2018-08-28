package test_test

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/kr/pretty"
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

// func TestOne(t *testing.T) {
// 	var opts []grpc.ServerOption
// 	stopChan := make(chan interface{})
// 	wg := &sync.WaitGroup{}
//
// 	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", port))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
//
// 	grpcServer := grpc.NewServer(opts...)
// 	messagesServer := server.NewMessagesServer()
// 	pb.RegisterMessagesServer(grpcServer, messagesServer)
//
// 	wg.Add(1)
// 	log.Printf("Starting agents gRPC server. Listening on %s", lis.Addr().String())
// 	runAgentsGRPCServer(grpcServer, lis, stopChan, wg)
//
// 	//
// 	apilis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", apiPort))
// 	if err != nil {
// 		t.Fatal(err)
// 	}
//
// 	apiGrpcServer := grpc.NewServer(opts...)
// 	apiServer := api.NewApiServer(messagesServer)
// 	pbapi.RegisterApiServer(apiGrpcServer, apiServer)
//
// 	wg.Add(1)
// 	log.Printf("Starting API gRPC server. Listening on %s", apilis.Addr().String())
// 	runAgentsGRPCServer(apiGrpcServer, apilis, stopChan, wg)
//
// 	clientOpts := []grpc.DialOption{grpc.WithInsecure()}
//
// 	agentServerAddr := fmt.Sprintf("127.0.0.1:%s", port)
// 	agentConn, err := grpc.Dial(agentServerAddr, clientOpts...)
//
// 	apiServerAddr := fmt.Sprintf("127.0.0.1:%s", apiPort)
// 	apiConn, err := grpc.Dial(apiServerAddr, clientOpts...)
// 	if err != nil {
// 		log.Fatalf("fail to dial: %v", err)
// 	}
// 	defer apiConn.Close()
//
// 	t1 := time.Now().Truncate(time.Second)
// 	clientID := "ABC123"
//
// 	time.Sleep(200 * time.Millisecond)
// 	agentClient := pb.NewMessagesClient(agentConn)
// 	agentStream, err := agentClient.MessagesChat(context.Background())
// 	registerMsg := &pb.ClientMessage{
// 		Type:     pb.ClientMessage_REGISTER,
// 		ClientID: clientID,
// 		Payload: &pb.ClientMessage_RegisterMsg{
// 			RegisterMsg: &pb.RegisterPayload{
// 				NodeType:  pb.NodeType_MONGOD,
// 				ClusterID: "",
// 			},
// 		},
// 	}
// 	err = agentStream.Send(registerMsg)
// 	if err != nil {
// 		t.Errorf("Cannot send register message: %s", err)
// 	}
// 	// Since the communication is asynchronous, give the server some time to register the client
// 	time.Sleep(200 * time.Millisecond)
//
// 	apiClient := pbapi.NewApiClient(apiConn)
// 	stream, err := apiClient.GetClients(context.Background(), &pbapi.Empty{})
// 	if err != nil {
// 		log.Fatalf("Cannot call GetClients: %s", err)
// 	}
//
// 	msg, err := stream.Recv()
// 	if err != nil {
// 		log.Fatalf("Error reading from the stream: %s", err)
// 	}
// 	if msg.ClientID != clientID {
// 		t.Errorf("Invalid client ID. Want %q, got %q", clientID, msg.ClientID)
// 	}
// 	if time.Unix(msg.LastSeen, 0).Before(t1) {
// 		t.Errorf("Invalid client last seen. Want > %v, got %v", t1, time.Unix(msg.LastSeen, 0))
// 	}
//
// 	close(stopChan)
// 	agentStream.CloseSend()
// 	wg.Wait()
// }

func TestTwo(t *testing.T) {
	var opts []grpc.ServerOption
	stopChan := make(chan interface{})
	wg := &sync.WaitGroup{}

	// Start the grpc server
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", port))
	if err != nil {
		t.Fatal(err)
	}

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

		agentID := fmt.Sprintf("PMB-%03d", i)

		client, err := client.NewClient(session, clientConn)
		if err != nil {
			t.Fatalf("Cannot create an agent instance %s: %s", agentID, err)
		}
		clients = append(clients, client)
	}

	clientsList := messagesServer.Clients()
	if testing.Verbose() {
		for key, client := range clientsList {
			fmt.Printf("Key: %s ************************\n", key)
			fmt.Printf("ID               : %s\n", client.ID)
			fmt.Printf("Node Type        : %v\n", client.NodeType)
			fmt.Printf("Node Name        : %v\n", client.NodeName)
			fmt.Printf("Cluster ID       : %v\n", client.ClusterID)
			fmt.Printf("Last command sent: %v\n", client.LastCommandSent)
			fmt.Printf("Last seen        : %v\n", client.LastSeen)
			fmt.Printf("Replicaset name  : %v\n", client.ReplicasetName)
			fmt.Printf("Replicaset ID    : %v\n", client.ReplicasetID)
			fmt.Printf("Status\n")
			fmt.Printf("   ReplicaSetVersion : %v\n", client.Status.ReplicasetVersion)
			fmt.Printf("   LastOplogTime     : %v\n", client.Status.LastOplogTs)
			fmt.Printf("   RunningDBBackup   : %v\n", client.Status.DBBackUpRunning)
		}
	}
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
		fmt.Printf("Cannot get first client status: %s\n", err)
		t.Errorf("Cannot get first client status: %s", err)
	}
	pretty.Println(status)

	time.Sleep(1 * time.Second)
	for _, client := range clients {
		client.Stop()
	}
	messagesServer.Stop()
	print("closing stopchan")
	close(stopChan)
	print("eait")
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
