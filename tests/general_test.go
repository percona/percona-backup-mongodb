package test_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/percona/mongodb-backup/grpc/api"
	"github.com/percona/mongodb-backup/grpc/server"
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

func TestOne(t *testing.T) {
	var opts []grpc.ServerOption
	stopChan := make(chan interface{})
	wg := &sync.WaitGroup{}

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", port))
	if err != nil {
		t.Fatal(err)
	}

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

	apiGrpcServer := grpc.NewServer(opts...)
	apiServer := api.NewApiServer(messagesServer)
	pbapi.RegisterApiServer(apiGrpcServer, apiServer)

	wg.Add(1)
	log.Printf("Starting API gRPC server. Listening on %s", apilis.Addr().String())
	runAgentsGRPCServer(apiGrpcServer, apilis, stopChan, wg)

	clientOpts := []grpc.DialOption{grpc.WithInsecure()}

	agentServerAddr := fmt.Sprintf("127.0.0.1:%s", port)
	agentConn, err := grpc.Dial(agentServerAddr, clientOpts...)

	apiServerAddr := fmt.Sprintf("127.0.0.1:%s", apiPort)
	apiConn, err := grpc.Dial(apiServerAddr, clientOpts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer apiConn.Close()

	t1 := time.Now().Truncate(time.Second)
	clientID := "ABC123"

	time.Sleep(200 * time.Millisecond)
	agentClient := pb.NewMessagesClient(agentConn)
	agentStream, err := agentClient.MessagesChat(context.Background())
	registerMsg := &pb.ClientMessage{
		Type:     pb.ClientMessage_REGISTER,
		ClientID: clientID,
		Message:  []byte(`{"node_type": "primary"}`),
		Payload:  &pb.ClientMessage_RegisterMsg{RegisterMsg: &pb.RegisterPayload{NodeType: "primary"}},
	}
	err = agentStream.Send(registerMsg)
	if err != nil {
		t.Errorf("Cannot send register message: %s", err)
	}
	// Since the communication is asynchronous, give the server some time to register the client
	time.Sleep(200 * time.Millisecond)

	apiClient := pbapi.NewApiClient(apiConn)
	stream, err := apiClient.GetClients(context.Background(), &pbapi.Empty{})
	if err != nil {
		log.Fatalf("Cannot call GetClients: %s", err)
	}

	msg, err := stream.Recv()
	if err != nil {
		log.Fatalf("Error reading from the stream: %s", err)
	}
	if msg.ClientID != clientID {
		t.Errorf("Invalid client ID. Want %q, got %q", clientID, msg.ClientID)
	}
	if time.Unix(msg.LastSeen, 0).Before(t1) {
		t.Errorf("Invalid client last seen. Want > %v, got %v", t1, time.Unix(msg.LastSeen, 0))
	}

	close(stopChan)
	agentStream.CloseSend()
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
