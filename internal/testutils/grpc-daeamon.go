package testutils

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/grpc/api"
	"github.com/percona/mongodb-backup/grpc/client"
	"github.com/percona/mongodb-backup/grpc/server"
	pbapi "github.com/percona/mongodb-backup/proto/api"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/prometheus/common/log"
	"github.com/sirupsen/logrus"
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
	MessagesServer     *server.MessagesServer
	ApiServer          *api.ApiServer
	msgListener        net.Listener
	apiListener        net.Listener
	wg                 *sync.WaitGroup
	ctx                context.Context
	cancelFunc         context.CancelFunc
	logger             *logrus.Logger
	lock               *sync.Mutex
	clients            []*client.Client
}

func NewGrpcDaemon(ctx context.Context, workDir string, t *testing.T, logger *logrus.Logger) (*GrpcDaemon, error) {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.StandardLogger().Level)
		logger.Out = logrus.StandardLogger().Out
	}
	var opts []grpc.ServerOption
	d := &GrpcDaemon{
		clients: make([]*client.Client, 0),
		wg:      &sync.WaitGroup{},
		logger:  logger,
		lock:    &sync.Mutex{},
	}
	var err error

	// Start the grpc server
	d.msgListener, err = net.Listen("tcp", fmt.Sprintf("localhost:%s", TEST_GRPC_MESSAGES_PORT))
	if err != nil {
		return nil, fmt.Errorf("cannot listen on port %s for the gRPC messages server", TEST_GRPC_MESSAGES_PORT)
	}

	d.ctx, d.cancelFunc = context.WithCancel(ctx)
	// This is the sever/agents gRPC server
	d.grpcServer4Clients = grpc.NewServer(opts...)
	d.MessagesServer = server.NewMessagesServer(workDir, logger)
	pb.RegisterMessagesServer(d.grpcServer4Clients, d.MessagesServer)

	d.wg.Add(1)
	logger.Printf("Starting agents gRPC server. Listening on %s", d.msgListener.Addr().String())
	d.runAgentsGRPCServer(d.ctx, d.grpcServer4Clients, d.msgListener, grpcServerShutdownTimeout, d.wg)

	//
	d.apiListener, err = net.Listen("tcp", fmt.Sprintf("localhost:%s", TEST_GRPC_API_PORT))
	if err != nil {
		return nil, fmt.Errorf("cannot listen on port %s for the gRPC API server", TEST_GRPC_API_PORT)
	}

	// This is the server gRPC API
	d.grpcServer4Api = grpc.NewServer(opts...)
	d.ApiServer = api.NewApiServer(d.MessagesServer)
	pbapi.RegisterApiServer(d.grpcServer4Api, d.ApiServer)

	d.wg.Add(1)
	logger.Printf("Starting API gRPC server. Listening on %s", d.apiListener.Addr().String())
	d.runAgentsGRPCServer(d.ctx, d.grpcServer4Api, d.apiListener, grpcServerShutdownTimeout, d.wg)

	clientOpts := []grpc.DialOption{grpc.WithInsecure()}

	clientServerAddr := fmt.Sprintf("127.0.0.1:%s", TEST_GRPC_MESSAGES_PORT)
	clientConn, err := grpc.Dial(clientServerAddr, clientOpts...)

	ports := []string{MongoDBShard1PrimaryPort, MongoDBShard1Secondary1Port, MongoDBShard1Secondary2Port,
		MongoDBShard2PrimaryPort, MongoDBShard2Secondary1Port, MongoDBShard2Secondary2Port,
		MongoDBConfigsvr1Port, // MongoDBConfigsvr2Port, MongoDBConfigsvr3Port,
		MongoDBMongosPort}
	repls := []string{MongoDBShard1ReplsetName, MongoDBShard1ReplsetName, MongoDBShard1ReplsetName,
		MongoDBShard2ReplsetName, MongoDBShard2ReplsetName, MongoDBShard2ReplsetName,
		MongoDBConfigsvrReplsetName, // MongoDBConfigsvrReplsetName, MongoDBConfigsvrReplsetName,
		""}

	for i, port := range ports {
		di := DialInfoForPort(t, repls[i], port)
		session, err := mgo.DialWithInfo(di)
		logger.Infof("Connecting agent #%d to: %s\n", i, di.Addrs[0])
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

		client, err := client.NewClient(d.ctx, workDir, dbConnOpts, client.SSLOptions{}, clientConn, logger)
		if err != nil {
			return nil, fmt.Errorf("Cannot create an agent instance %s: %s", agentID, err)
		}
		d.clients = append(d.clients, client)
	}

	return d, nil
}

func (d *GrpcDaemon) Stop() {
	d.lock.Lock()
	defer d.lock.Unlock()

	for _, client := range d.clients {
		if err := client.Stop(); err != nil {
			log.Errorf("Cannot stop client %s: %s", client.ID(), err)
		}
	}
	d.MessagesServer.Stop()
	d.cancelFunc()
	d.wg.Wait()
	d.msgListener.Close()
	d.apiListener.Close()
}

func (d *GrpcDaemon) ClientsCount() int {
	d.lock.Lock()
	defer d.lock.Unlock()
	return len(d.clients)
}

func (d *GrpcDaemon) Clients() []*client.Client {
	return d.clients
}

func (d *GrpcDaemon) runAgentsGRPCServer(ctx context.Context, grpcServer *grpc.Server, lis net.Listener, shutdownTimeout int, wg *sync.WaitGroup) {
	go func() {
		err := grpcServer.Serve(lis)
		if err != nil {
			d.logger.Printf("Cannot start agents gRPC server: %s", err)
		}
		d.logger.Println("Stopping server " + lis.Addr().String())
		wg.Done()
	}()

	go func() {
		<-ctx.Done()
		d.logger.Printf("Gracefuly stopping server at %s", lis.Addr().String())
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

	time.Sleep(2 * time.Second)
}
