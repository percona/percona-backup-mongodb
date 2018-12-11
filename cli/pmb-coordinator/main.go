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
	"github.com/percona/mongodb-backup/internal/logger"
	apipb "github.com/percona/mongodb-backup/proto/api"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
	yaml "gopkg.in/yaml.v2"
)

// vars are set by goreleaser
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
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
	GrpcBindIP           string `yaml:"grpc_bindip"`
	GrpcPort             int    `yaml:"grpc_port"`
	APIBindIP            string `yaml:"api_bindip"`
	APIPort              int    `yaml:"api_port"`
	ShutdownTimeout      int    `yaml:"shutdown_timeout"`
	LogFile              string `yaml:"log_file"`
	UseSysLog            bool   `yaml:"sys_log_url"`
	Debug                bool   `yaml:"debug"`
	EnableClientsLogging bool   `yaml:"enable_clients_logging"`
}

const (
	defaultGrpcPort        = 10000
	defaultAPIPort         = 10001
	defaultShutdownTimeout = 5 // Seconds
	defaultClientsLogging  = true
	defaultDebugMode       = true
	defaultWorkDir         = "~/percona-mongodb-backup"
)

var (
	log = logrus.New()
)

func main() {
	opts, err := processCliParams()
	if err != nil {
		log.Fatalf("Cannot parse command line arguments: %s", err)
	}

	if opts.UseSysLog {
		log = logger.NewSyslogLogger()
	} else {
		log = logger.NewDefaultLogger(opts.LogFile)
	}

	if opts.Debug {
		log.SetLevel(logrus.DebugLevel)
	}

	log.Infof("Starting clients gRPC net listener at address: %s:%d", opts.GrpcBindIP, opts.GrpcPort)
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", opts.GrpcBindIP, opts.GrpcPort))

	log.Infof("Starting clients API net listener at address: %s:%d", opts.APIBindIP, opts.APIPort)
	apilis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", opts.APIBindIP, opts.APIPort))

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
	app := kingpin.New("pmb-coordinator", "Percona MongoDB backup coordinator")
	app.Version(fmt.Sprintf("%s version %s, git commit %s", app.Name, version, commit))

	opts := &cliOptions{
		app: app,
	}

	app.Flag("config-file", "Config file").Default("config.yml").StringVar(&opts.configFile)
	app.Flag("tls", "Enable TLS").BoolVar(&opts.TLS)
	app.Flag("work-dir", "Working directory for backup metadata").StringVar(&opts.WorkDir)
	app.Flag("cert-file", "Cert file for gRPC client connections").StringVar(&opts.CertFile)
	app.Flag("key-file", "Key file for gRPC client connections").StringVar(&opts.KeyFile)
	app.Flag("grpc-bindip", "Bind IP for gRPC client connections").StringVar(&opts.GrpcBindIP)
	app.Flag("grpc-port", "Listening port for gRPC client connections").IntVar(&opts.GrpcPort)
	app.Flag("api-bindip", "Bind IP for API client connections").StringVar(&opts.APIBindIP)
	app.Flag("api-port", "Listening port for API client connections").IntVar(&opts.APIPort)
	app.Flag("shutdown-timeout", "Server shutdown timeout").IntVar(&opts.ShutdownTimeout)
	app.Flag("log-file", "Write logs to file").StringVar(&opts.LogFile)
	app.Flag("use-syslog", "Also send the logs to the local syslog server").BoolVar(&opts.UseSysLog)
	app.Flag("debug", "Enable debug log level").BoolVar(&opts.Debug)
	app.Flag("enable-clients-logging", "Enable showing logs comming from agents on the server side").BoolVar(&opts.EnableClientsLogging)

	opts.cmd, err = app.DefaultEnvars().Parse(os.Args[1:])
	if err != nil {
		return nil, err
	}

	yamlOpts := &cliOptions{
		GrpcPort:             defaultGrpcPort,
		APIPort:              defaultAPIPort,
		ShutdownTimeout:      defaultShutdownTimeout,
		Debug:                defaultDebugMode,
		WorkDir:              defaultWorkDir,
		EnableClientsLogging: defaultClientsLogging,
	}
	if opts.configFile != "" {
		loadOptionsFromFile(expandHomeDir(opts.configFile), yamlOpts)
	}

	mergeOptions(opts, yamlOpts)
	expandDirs(yamlOpts)
	// Return yamlOpts instead of opts because it has the defaults + the command line parameters
	// we want to overwrite
	if err = checkWorkDir(yamlOpts.WorkDir); err != nil {
		return nil, err
	}
	return yamlOpts, err
}

func checkWorkDir(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		log.Infof("Work dir %s doesn't exist. Creating it", dir)
		return os.MkdirAll(dir, os.ModePerm)
	}
	if !fi.IsDir() {
		return fmt.Errorf("Cannot use %s for backups metadata. It is not a directory", dir)
	}
	return err
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
	if opts.APIPort != 0 {
		yamlOpts.APIPort = opts.APIPort
	}
	if opts.ShutdownTimeout != 0 {
		yamlOpts.ShutdownTimeout = opts.ShutdownTimeout
	}
	if opts.EnableClientsLogging != false {
		yamlOpts.EnableClientsLogging = opts.EnableClientsLogging
	}
	if opts.UseSysLog == true {
		yamlOpts.UseSysLog = true
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
	dir := os.Getenv("HOME")
	usr, err := user.Current()
	if err == nil {
		dir = usr.HomeDir
	}
	if path == "~" {
		return dir
	}
	if strings.HasPrefix(path, "~/") {
		return filepath.Join(dir, path[2:])
	}
	return path
}
