package testutils

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"sync"
	"time"

	"github.com/globalsign/mgo"
	"github.com/kr/pretty"
	"github.com/percona/mongodb-backup/grpc/api"
	"github.com/percona/mongodb-backup/grpc/client"
	"github.com/percona/mongodb-backup/grpc/server"
	pbapi "github.com/percona/mongodb-backup/proto/api"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"google.golang.org/grpc"
)

const (
	TEST_GRPC_MESSAGES_PORT = "10000"
	TEST_GRPC_API_PORT      = "10001"
)

var grpcServerShutdownTimeout = 30

type GrpcDaemon struct {
	grpcServer4Api     *grpc.Server
	grpcServer4Clients *grpc.Server
	messagesServer     *server.MessagesServer
	ApiServer          *api.ApiServer
	msgListener        net.Listener
	apiListener        net.Listener
	clients            []*client.Client
	wg                 *sync.WaitGroup
	ctx                context.Context
	cancelFunc         context.CancelFunc
}

func NewGrpcDaemon(ctx context.Context) (*GrpcDaemon, error) {
	var opts []grpc.ServerOption
	d := &GrpcDaemon{
		clients: make([]*client.Client, 0),
		wg:      &sync.WaitGroup{},
	}

	tmpDir := path.Join(os.TempDir(), "dump_test")
	os.RemoveAll(tmpDir) // Don't check for errors. The path might not exist
	err := os.MkdirAll(tmpDir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("Cannot create temp dir %q, %s", tmpDir, err)
	}

	// Start the grpc server
	d.msgListener, err = net.Listen("tcp", fmt.Sprintf("localhost:%s", TEST_GRPC_MESSAGES_PORT))
	if err != nil {
		return nil, fmt.Errorf("cannot listen on port %s for the gRPC messages server", TEST_GRPC_MESSAGES_PORT)
	}

	d.ctx, d.cancelFunc = context.WithCancel(ctx)
	// This is the sever/agents gRPC server
	d.grpcServer4Clients = grpc.NewServer(opts...)
	d.messagesServer = server.NewMessagesServer()
	pb.RegisterMessagesServer(d.grpcServer4Clients, d.messagesServer)

	d.wg.Add(1)
	log.Printf("Starting agents gRPC server. Listening on %s", d.msgListener.Addr().String())
	d.runAgentsGRPCServer(d.ctx, d.grpcServer4Clients, d.msgListener, grpcServerShutdownTimeout, d.wg)

	//
	d.apiListener, err = net.Listen("tcp", fmt.Sprintf("localhost:%s", TEST_GRPC_API_PORT))
	if err != nil {
		return nil, fmt.Errorf("cannot listen on port %s for the gRPC API server", TEST_GRPC_API_PORT)
	}

	// This is the server gRPC API
	d.grpcServer4Api = grpc.NewServer(opts...)
	d.ApiServer = api.NewApiServer(d.messagesServer)
	pbapi.RegisterApiServer(d.grpcServer4Api, d.ApiServer)

	d.wg.Add(1)
	log.Printf("Starting API gRPC server. Listening on %s", d.apiListener.Addr().String())
	d.runAgentsGRPCServer(d.ctx, d.grpcServer4Api, d.apiListener, grpcServerShutdownTimeout, d.wg)

	clientOpts := []grpc.DialOption{grpc.WithInsecure()}

	clientServerAddr := fmt.Sprintf("127.0.0.1:%s", TEST_GRPC_MESSAGES_PORT)
	clientConn, err := grpc.Dial(clientServerAddr, clientOpts...)

	ports := []string{MongoDBPrimaryPort, MongoDBSecondary1Port, MongoDBSecondary2Port}

	for i, port := range ports {
		di := DialInfoForPort(port)
		pretty.Println(di)
		session, err := mgo.DialWithInfo(di)
		log.Printf("Connecting agent #%d to: %s\n", i, di.Addrs[0])
		if err != nil {
			return nil, fmt.Errorf("cannot create a new agent; cannot connect to the MongoDB server %q: %s", di.Addrs[0], err)
		}
		session.SetMode(mgo.Eventual, true)

		agentID := fmt.Sprintf("PMB-%03d", i)

		dbConnOpts := client.ConnectionOptions{
			Host:           MongoDBHost,
			Port:           port,
			User:           di.Username,
			Password:       di.Password,
			ReplicasetName: di.ReplicaSetName,
		}

		client, err := client.NewClient(d.ctx, dbConnOpts, client.SSLOptions{}, clientConn)
		if err != nil {
			return nil, fmt.Errorf("Cannot create an agent instance %s: %s", agentID, err)
		}
		d.clients = append(d.clients, client)
	}

	return d, nil
}

func (d *GrpcDaemon) Stop() {
	d.cancelFunc()
	d.messagesServer.Stop()
	d.wg.Wait()
	d.msgListener.Close()
	d.apiListener.Close()
}

func (d *GrpcDaemon) runAgentsGRPCServer(ctx context.Context, grpcServer *grpc.Server, lis net.Listener, shutdownTimeout int, wg *sync.WaitGroup) {
	go func() {
		err := grpcServer.Serve(lis)
		if err != nil {
			log.Printf("Cannot start agents gRPC server: %s", err)
		}
		log.Println("Stopping server " + lis.Addr().String())
		wg.Done()
	}()

	go func() {
		<-ctx.Done()
		log.Printf("Gracefuly stopping server at %s", lis.Addr().String())
		// Try to Gracefuly stop the gRPC server.
		c := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			c <- struct{}{}
		}()

		// If after shutdownTimeout seconds the server hasn't stop, just kill it.
		select {
		case <-c:
			return
		case <-time.After(time.Duration(shutdownTimeout) * time.Second):
			grpcServer.Stop()
		}
	}()
}
