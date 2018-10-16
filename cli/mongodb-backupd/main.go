package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/percona/mongodb-backup/grpc/api"
	"github.com/percona/mongodb-backup/grpc/server"
	apipb "github.com/percona/mongodb-backup/proto/api"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
	yaml "gopkg.in/yaml.v2"
)

type cliOptions struct {
	app        *kingpin.Application
	cmd        string
	configFile string
	//
	TLS                  bool   `yaml:"tls"`
	WorkDir              string `yaml:"work_dir"`
	CertFile             string `yaml:"cert_file"`
	KeyFile              string `yaml:"key_file"`
	GrpcPort             int    `yaml:"grpc_port"`
	ApiPort              int    `yaml:"api_port"`
	ShutdownTimeout      int    `yaml:"shutdown_timeout"`
	Debug                bool   `yaml:"debug"`
	EnableClientsLogging bool   `yaml:"enable_clients_logging"`
}

var (
	log = logrus.New()
)

const (
	defaultGrpcPort        = 10000
	defaultAPIPort         = 10001
	defaultShutdownTimeout = 5 // Seconds
	defaultClientsLogging  = true
	defaultDebugMode       = true
)

func main() {
	opts, err := processCliParams()
	if err != nil {
		log.Fatalf("Cannot parse command line arguments: %s", err)
	}
	if opts.Debug {
		log.SetLevel(logrus.DebugLevel)
	}

	log.Infof("Starting clients gRPC net listener on port: %d", opts.GrpcPort)
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", opts.GrpcPort))

	apilis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", opts.ApiPort))

	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var grpcOpts []grpc.ServerOption
	if opts.TLS {
		if opts.CertFile == "" {
			opts.CertFile = testdata.Path("server1.pem")
		}
		if opts.KeyFile == "" {
			opts.KeyFile = testdata.Path("server1.key")
		}
		creds, err := credentials.NewServerTLSFromFile(opts.CertFile, opts.KeyFile)
		if err != nil {
			log.Fatalf("Failed to generate credentials %v", err)
		}
		grpcOpts = []grpc.ServerOption{grpc.Creds(creds)}
	}

	stopChan := make(chan interface{})
	wg := &sync.WaitGroup{}

	var messagesServer *server.MessagesServer
	grpcServer := grpc.NewServer(grpcOpts...)
	if opts.EnableClientsLogging {
		messagesServer = server.NewMessagesServerWithClientLogging(opts.WorkDir, log)
	} else {
		messagesServer = server.NewMessagesServer(opts.WorkDir, log)
	}
	pb.RegisterMessagesServer(grpcServer, messagesServer)

	wg.Add(1)
	log.Printf("Starting agents gRPC server. Listening on %s", lis.Addr().String())
	runAgentsGRPCServer(grpcServer, lis, opts.ShutdownTimeout, stopChan, wg)

	apiGrpcServer := grpc.NewServer(grpcOpts...)
	apiServer := api.NewApiServer(messagesServer)
	apipb.RegisterApiServer(apiGrpcServer, apiServer)

	wg.Add(1)
	log.Infof("Starting API gRPC server. Listening on %s", apilis.Addr().String())
	runAgentsGRPCServer(apiGrpcServer, apilis, opts.ShutdownTimeout, stopChan, wg)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	<-c
	log.Infof("Stop signal received. Stopping the server")
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

func processCliParams() (*cliOptions, error) {
	var err error
	app := kingpin.New("percona-mongodb-backupd", "Percona MongoDB backup server")
	opts := &cliOptions{
		app: app,
	}

	app.Flag("config-file", "Config file").Default("config.yml").StringVar(&opts.configFile)
	app.Flag("tls", "Enable TLS").BoolVar(&opts.TLS)
	app.Flag("work-dir", "Working directory for backup metadata").StringVar(&opts.WorkDir)
	app.Flag("cert-file", "Cert file for gRPC client connections").StringVar(&opts.CertFile)
	app.Flag("key-file", "Key file for gRPC client connections").StringVar(&opts.KeyFile)
	app.Flag("grpc-port", "Listening port for client connections").IntVar(&opts.GrpcPort)
	app.Flag("api-port", "Listening por for API client connecions").IntVar(&opts.ApiPort)
	app.Flag("shutdown-timeout", "Server shutdown timeout").IntVar(&opts.ShutdownTimeout)
	app.Flag("debug", "Enable debug log level").BoolVar(&opts.Debug)
	app.Flag("enable-clients-logging", "Enable showing logs comming from agents on the server side").BoolVar(&opts.EnableClientsLogging)

	opts.cmd, err = app.Parse(os.Args[1:])
	if err != nil {
		return nil, err
	}

	yamlOpts := &cliOptions{
		GrpcPort:             defaultGrpcPort,
		ApiPort:              defaultAPIPort,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		EnableClientsLogging: defaultClientsLogging,
	}
	if opts.configFile != "" {
		err = loadOptionsFromFile(expandHomeDir(opts.configFile), yamlOpts)
	}
	mergeOptions(opts, yamlOpts)
	expandDirs(yamlOpts)
	// Return yamlOpts instead of opts because it has the defaults + the command line parameters
	// we want to overwrite
	return yamlOpts, err
}

func loadOptionsFromFile(filename string, opts *cliOptions) error {
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return errors.Wrap(err, "cannot load configuration from file")
	}
	if err = yaml.Unmarshal(buf, opts); err != nil {
		return errors.Wrapf(err, "cannot unmarshal yaml file %s", filename)
	}
	return nil
}

func mergeOptions(opts, yamlOpts *cliOptions) {
	if opts.TLS != false {
		yamlOpts.TLS = opts.TLS
	}
	if opts.WorkDir != "" {
		yamlOpts.WorkDir = opts.WorkDir
	}
	if opts.CertFile != "" {
		yamlOpts.CertFile = opts.CertFile
	}
	if opts.KeyFile != "" {
		yamlOpts.KeyFile = opts.KeyFile
	}
	if opts.GrpcPort != 0 {
		yamlOpts.GrpcPort = opts.GrpcPort
	}
	if opts.ApiPort != 0 {
		yamlOpts.ApiPort = opts.ApiPort
	}
	if opts.ShutdownTimeout != 0 {
		yamlOpts.ShutdownTimeout = opts.ShutdownTimeout
	}
	if opts.EnableClientsLogging != false {
		yamlOpts.EnableClientsLogging = opts.EnableClientsLogging
	}
	if opts.Debug != false {
		yamlOpts.Debug = opts.Debug
	}
}

func expandDirs(opts *cliOptions) {
	opts.WorkDir = expandHomeDir(opts.WorkDir)
	opts.CertFile = expandHomeDir(opts.CertFile)
	opts.KeyFile = expandHomeDir(opts.KeyFile)
}

func expandHomeDir(path string) string {
	usr, _ := user.Current()
	dir := usr.HomeDir
	if path == "~" {
		return dir
	}
	if strings.HasPrefix(path, "~/") {
		return filepath.Join(dir, path[2:])
	}
	return path
}
