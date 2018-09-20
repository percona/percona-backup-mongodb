package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/percona/mongodb-backup/grpc/api"
	"github.com/percona/mongodb-backup/grpc/server"
	apipb "github.com/percona/mongodb-backup/proto/api"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
)

type cliOptions struct {
	app             *kingpin.Application
	cmd             string
	tls             *bool
	certFile        *string
	keyFile         *string
	grpcPort        *int
	apiPort         *int
	shutdownTimeout *int
	debug           *bool
}

var (
	log = logrus.New()
)

const (
	defaultGrpcPort = "10000"
	defaultAPIPort  = "10001"
)

func processCliParams() (*cliOptions, error) {
	var err error
	app := kingpin.New("percona-mongodb-backupd", "Percona MongoDB backup server")
	opts := &cliOptions{
		app:             app,
		tls:             app.Flag("tls", "Enable TLS").Bool(),
		certFile:        app.Flag("cert-file", "Cert file for gRPC client connections").String(),
		keyFile:         app.Flag("key-file", "Key file for gRPC client connections").String(),
		grpcPort:        app.Flag("grpc-port", "Listening port for client connections").Default(defaultGrpcPort).Int(),
		apiPort:         app.Flag("api-port", "Listening por for API client connecions").Default(defaultAPIPort).Int(),
		shutdownTimeout: app.Flag("shutdown-timeout", "Server shutdown timeout").Default("3").Int(),
		debug:           app.Flag("debug", "Enable debug log level").Bool(),
	}

	opts.cmd, err = app.Parse(os.Args[1:])
	if err != nil {
		return nil, err
	}
	return opts, err
}

func main() {
	opts, err := processCliParams()
	if err != nil {
		log.Fatalf("Cannot parse command line arguments: %s", err)
	}
	if *opts.debug {
		log.SetLevel(logrus.DebugLevel)
	}

	log.Infof("Starting clients gRPC net listener on port: %d", *opts.grpcPort)
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *opts.grpcPort))

	apilis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *opts.apiPort))

	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var grpcOpts []grpc.ServerOption
	if *opts.tls {
		if *opts.certFile == "" {
			*opts.certFile = testdata.Path("server1.pem")
		}
		if *opts.keyFile == "" {
			*opts.keyFile = testdata.Path("server1.key")
		}
		creds, err := credentials.NewServerTLSFromFile(*opts.certFile, *opts.keyFile)
		if err != nil {
			log.Fatalf("Failed to generate credentials %v", err)
		}
		grpcOpts = []grpc.ServerOption{grpc.Creds(creds)}
	}

	stopChan := make(chan interface{})
	wg := &sync.WaitGroup{}

	grpcServer := grpc.NewServer(grpcOpts...)
	messagesServer := server.NewMessagesServer(log)
	pb.RegisterMessagesServer(grpcServer, messagesServer)

	wg.Add(1)
	log.Printf("Starting agents gRPC server. Listening on %s", lis.Addr().String())
	runAgentsGRPCServer(grpcServer, lis, *opts.shutdownTimeout, stopChan, wg)

	apiGrpcServer := grpc.NewServer(grpcOpts...)
	apiServer := api.NewApiServer(messagesServer)
	apipb.RegisterApiServer(apiGrpcServer, apiServer)

	wg.Add(1)
	log.Infof("Starting API gRPC server. Listening on %s", apilis.Addr().String())
	runAgentsGRPCServer(apiGrpcServer, apilis, *opts.shutdownTimeout, stopChan, wg)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	<-c
	log.Infof("Stop signal received. Stopping the agent")
	close(stopChan)
	wg.Wait()
}

func runAgentsGRPCServer(grpcServer *grpc.Server, lis net.Listener, shutdownTimeout int, stopChan chan interface{}, wg *sync.WaitGroup) {
	go func() {
		fmt.Printf("Starting grpc server on port %v\n", lis.Addr().String())
		err := grpcServer.Serve(lis)
		if err != nil {
			log.Printf("Cannot start agents gRPC server: %s", err)
		}
		wg.Done()
	}()

	go func() {
		<-stopChan
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
			log.Printf("Stopping server at %s", lis.Addr().String())
			grpcServer.Stop()
		}
	}()
}
