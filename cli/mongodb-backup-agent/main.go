package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/alecthomas/kingpin"
	"github.com/globalsign/mgo"
	"github.com/percona/mongodb-backup/grpc/client"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type cliOptios struct {
	app           *kingpin.Application
	dsn           *string
	pidFile       *string
	serverAddress *string
	backupDir     *string
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
	log := logrus.New()
	opts, err := processCliArgs()
	if err != nil {
		log.Fatalf("Cannot parse command line arguments: %s", err)
	}

	log.SetLevel(logrus.InfoLevel)
	if *opts.quiet {
		log.SetLevel(logrus.ErrorLevel)
	}
	if *opts.debug {
		log.SetLevel(logrus.DebugLevel)
	}
	log.SetLevel(logrus.DebugLevel)

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
			Addrs:          []string{opts.mongodbConnOptions.Host + ":" + opts.mongodbConnOptions.Port},
			Username:       opts.mongodbConnOptions.User,
			Password:       opts.mongodbConnOptions.Password,
			ReplicaSetName: opts.mongodbConnOptions.ReplicasetName,
			FailFast:       true,
			Source:         "admin",
		}
	} else {
		di, err = mgo.ParseURL(*opts.dsn)
		di.FailFast = true
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

	client, err := client.NewClient(context.Background(), *opts.backupDir, opts.mongodbConnOptions, opts.mongodbSslOptions, conn, log)
	if err != nil {
		log.Fatal(err)
	}

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
		serverAddress: app.Flag("server-address", "MongoDB backup server address").Default("127.0.0.1:10000").String(),
		backupDir:     app.Flag("backup-dir", "Directory where to store the backups").Default("/tmp").String(),
		tls:           app.Flag("tls", "Use TLS").Bool(),
		pidFile:       app.Flag("pid-file", "pid file").String(),
		caFile:        app.Flag("ca-file", "CA file").String(),
		debug:         app.Flag("debug", "Enable debug log level").Bool(),
		quiet:         app.Flag("quiet", "Quiet mode. Log only errors").Bool(),
	}

	//TODO: Remove defaults. These values are only here to test during development
	// These params should be Required()
	app.Flag("mongodb-host", "MongoDB host").Default("127.0.0.1").StringVar(&opts.mongodbConnOptions.Host)
	app.Flag("mongodb-port", "MongoDB port").Default(os.Getenv("TEST_MONGODB_S1_PRIMARY_PORT")).StringVar(&opts.mongodbConnOptions.Port)
	app.Flag("mongodb-user", "MongoDB username").Default(os.Getenv("TEST_MONGODB_USERNAME")).StringVar(&opts.mongodbConnOptions.User)
	app.Flag("mongodb-password", "MongoDB password").Default(os.Getenv("TEST_MONGODB_PASSWORD")).StringVar(&opts.mongodbConnOptions.Password)
	app.Flag("replicaset", "Replicaset name").Default(os.Getenv("TEST_MONGODB_S1_RS")).StringVar(&opts.mongodbConnOptions.ReplicasetName)

	_, err := app.Parse(os.Args[1:])
	if err != nil {
		return nil, err
	}

	if *opts.pidFile != "" {
		if err := writePidFile(*opts.pidFile); err != nil {
			return nil, errors.Wrapf(err, "cannot write pid file %q", *opts.pidFile)
		}
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

	fi, err := os.Stat(*opts.backupDir)
	if err != nil {
		return nil, errors.Wrap(err, "invalid backup destination dir")
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("%q is not a directory", *opts.backupDir)
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

// Write a pid file, but first make sure it doesn't exist with a running pid.
func writePidFile(pidFile string) error {
	// Read in the pid file as a slice of bytes.
	if piddata, err := ioutil.ReadFile(pidFile); err == nil {
		// Convert the file contents to an integer.
		if pid, err := strconv.Atoi(string(piddata)); err == nil {
			// Look for the pid in the process list.
			if process, err := os.FindProcess(pid); err == nil {
				// Send the process a signal zero kill.
				if err := process.Signal(syscall.Signal(0)); err == nil {
					// We only get an error if the pid isn't running, or it's not ours.
					return fmt.Errorf("pid already running: %d", pid)
				}
			}
		}
	}
	// If we get here, then the pidfile didn't exist,
	// or the pid in it doesn't belong to the user running this app.
	return ioutil.WriteFile(pidFile, []byte(fmt.Sprintf("%d", os.Getpid())), 0664)
}
