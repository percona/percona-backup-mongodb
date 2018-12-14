package grpc

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/globalsign/mgo"
	"github.com/percona/percona-backup-mongodb/grpc/api"
	"github.com/percona/percona-backup-mongodb/grpc/client"
	"github.com/percona/percona-backup-mongodb/grpc/server"
	"github.com/percona/percona-backup-mongodb/internal/testutils"
	pbapi "github.com/percona/percona-backup-mongodb/proto/api"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
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
		logger = &logrus.Logger{
			Out: os.Stderr,
			Formatter: &logrus.TextFormatter{
				FullTimestamp:          true,
				DisableLevelTruncation: true,
			},
			Hooks: make(logrus.LevelHooks),
			Level: logrus.DebugLevel,
		}
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

	ports := []string{testutils.MongoDBShard1PrimaryPort, testutils.MongoDBShard1Secondary1Port, testutils.MongoDBShard1Secondary2Port,
		testutils.MongoDBShard2PrimaryPort, testutils.MongoDBShard2Secondary1Port, testutils.MongoDBShard2Secondary2Port,
		testutils.MongoDBConfigsvr1Port, // testutils.MongoDBConfigsvr2Port, testutils.MongoDBConfigsvr3Port,
		testutils.MongoDBMongosPort}
	repls := []string{testutils.MongoDBShard1ReplsetName, testutils.MongoDBShard1ReplsetName, testutils.MongoDBShard1ReplsetName,
		testutils.MongoDBShard2ReplsetName, testutils.MongoDBShard2ReplsetName, testutils.MongoDBShard2ReplsetName,
		testutils.MongoDBConfigsvrReplsetName, // testutils.MongoDBConfigsvrReplsetName, testutils.MongoDBConfigsvrReplsetName,
		""}

	for i, port := range ports {
		di := testutils.DialInfoForPort(t, repls[i], port)
		session, err := mgo.DialWithInfo(di)
		logger.Infof("Connecting agent #%d to: %s\n", i, di.Addrs[0])
		if err != nil {
			return nil, fmt.Errorf("cannot create a new agent; cannot connect to the MongoDB server %q: %s", di.Addrs[0], err)
		}
		session.SetMode(mgo.Eventual, true)

		agentID := fmt.Sprintf("PMB-%03d", i)

		dbConnOpts := client.ConnectionOptions{
			Host:           testutils.MongoDBHost,
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

func (d *GrpcDaemon) APIClient() *grpc.Server {
	return d.grpcServer4Api
}

func (d *GrpcDaemon) MessagesClient() *grpc.Server {
	return d.grpcServer4Clients
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
