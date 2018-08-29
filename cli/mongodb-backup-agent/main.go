package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"

	"github.com/alecthomas/kingpin"
	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/grpc/client"
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
	defer mdbSession.Close()

	client, err := client.NewClient(mdbSession, conn)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	<-c
	client.Stop()
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
