package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/percona/mongodb-backup/grpc/api"
	"github.com/percona/mongodb-backup/grpc/server"
	apipb "github.com/percona/mongodb-backup/proto/api"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
)

var (
	tls        = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile   = flag.String("cert_file", "", "The TLS cert file")
	keyFile    = flag.String("key_file", "", "The TLS key file")
	jsonDBFile = flag.String("json_db_file", "testdata/route_guide_db.json", "A json file containing a list of features")
	port       = flag.Int("port", 10000, "The server port")
	apiPort    = flag.Int("api-port", 10001, "The server port")

	shutdownTimeout = 5 * time.Second
)

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	apilis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *apiPort))

	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	if *tls {
		if *certFile == "" {
			*certFile = testdata.Path("server1.pem")
		}
		if *keyFile == "" {
			*keyFile = testdata.Path("server1.key")
		}
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			log.Fatalf("Failed to generate credentials %v", err)
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}

	stopChan := make(chan interface{})
	wg := &sync.WaitGroup{}

	//
	grpcServer := grpc.NewServer(opts...)
	messagesServer := server.NewMessagesServer()
	pb.RegisterMessagesServer(grpcServer, messagesServer)

	wg.Add(1)
	log.Printf("Starting agents gRPC server. Listening on %s", lis.Addr().String())
	runAgentsGRPCServer(grpcServer, lis, stopChan, wg)

	apiGrpcServer := grpc.NewServer(opts...)
	apiServer := api.NewApiServer(messagesServer)
	apipb.RegisterApiServer(apiGrpcServer, apiServer)

	//
	wg.Add(1)
	log.Printf("Starting API gRPC server. Listening on %s", apilis.Addr().String())
	runAgentsGRPCServer(apiGrpcServer, apilis, stopChan, wg)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	<-c
	close(stopChan)
	wg.Wait()
}

func runAgentsGRPCServer(grpcServer *grpc.Server, lis net.Listener, stopChan chan interface{}, wg *sync.WaitGroup) {
	go func() {
		err := grpcServer.Serve(lis)
		if err != nil {
			log.Printf("Cannot start agents gRPC server: %s", err)
		}
		wg.Done()
	}()

	go func() {
		<-stopChan
		log.Printf("Gracefuly stopping server at %s", lis.Addr().String())
		grpcServer.GracefulStop()
	}()
}
