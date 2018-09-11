package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"

	"github.com/alecthomas/kingpin"
	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/grpc/client"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type cliOptios struct {
	app           *kingpin.Application
	dsn           *string
	serverAddress *string
	tls           *bool
	caFile        *string
	debug         *bool
	quiet         *bool

	// MongoDB connection options
	mongodbConnOptions struct {
		Host                string
		Port                string
		User                string
		Password            string
		ReplicasetName      string
		Timeout             int
		TCPKeepAliveSeconds int
	}

	// MongoDB connection SSL options
	mongodbSslOptions struct {
		UseSSL              bool
		SSLCAFile           string
		SSLPEMKeyFile       string
		SSLPEMKeyPassword   string
		SSLCRLFile          string
		SSLAllowInvalidCert bool
		SSLAllowInvalidHost bool
		SSLFipsMode         bool
	}
}

func main() {
	opts, err := processCliArgs()
	if err != nil {
		log.Fatalf("Cannot parse command line arguments: %s", err)
	}

	log.SetLevel(log.InfoLevel)
	if *opts.quiet {
		log.SetLevel(log.ErrorLevel)
	}
	if *opts.debug {
		log.SetLevel(log.DebugLevel)
	}

	grpcOpts := getgRPCOptions(opts)
	clientID := fmt.Sprintf("ABC%04d", rand.Int63n(10000))
	log.Infof("Using Client ID: %s", clientID)

	// Connect to the mongodb-backup gRPC server
	conn, err := grpc.Dial(*opts.serverAddress, grpcOpts...)
	if err != nil {
		log.Fatalf("Fail to connect to the gRPC server at %q: %v", *opts.serverAddress, err)
	}
	defer conn.Close()
	log.Infof("Connected to the gRPC server at %s", *opts.serverAddress)

	// Connect to the MongoDB instance
	var di *mgo.DialInfo
	if *opts.dsn == "" {
		di = &mgo.DialInfo{
			Addrs: []string{opts.mongodbConnOptions.Host + ":" + opts.mongodbConnOptions.Port},
		}
	} else {
		di, err = mgo.ParseURL(*opts.dsn)
		if err != nil {
			log.Fatalf("Cannot parse MongoDB DSN %q, %s", *opts.dsn, err)
		}
	}
	mdbSession, err := mgo.DialWithInfo(di)
	if err != nil {
		log.Fatalf("Cannot connect to MongoDB at %s: %s", di.Addrs[0], err)
	}
	defer mdbSession.Close()
	log.Infof("Connected to MongoDB at %s", di.Addrs[0])

	client, err := client.NewClient(context.Background(), opts.mongodbConnOptions, opts.mongodbSslOptions, conn)
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
		debug:         app.Flag("debug", "Enable debug log level").Bool(),
		quiet:         app.Flag("quiet", "Quiet mode. Log only errors").Bool(),
	}

	app.Flag("mongodb-host", "MongoDB host").StringVar(&opts.mongodbConnOptions.Host)
	app.Flag("mongodb-port", "MongoDB port").StringVar(&opts.mongodbConnOptions.Port)
	app.Flag("mongodb-user", "MongoDB username").StringVar(&opts.mongodbConnOptions.User)
	app.Flag("mongodb-password", "MongoDB password").StringVar(&opts.mongodbConnOptions.Password)
	app.Flag("replicaset", "Replicaset name").StringVar(&opts.mongodbConnOptions.ReplicasetName)

	_, err := app.Parse(os.Args[1:])
	if err != nil {
		return nil, err
	}

	if *opts.dsn != "" {
		di, err := mgo.ParseURL(*opts.dsn)
		if err != nil {
			return nil, err
		}
		parts := strings.Split(di.Addrs[0], ":")
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid host:port: %s", di.Addrs[0])
		}
		opts.mongodbConnOptions.Host = parts[0]
		opts.mongodbConnOptions.Port = parts[1]
		opts.mongodbConnOptions.User = di.Username
		opts.mongodbConnOptions.Password = di.Password
		opts.mongodbConnOptions.ReplicasetName = di.ReplicaSetName
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
