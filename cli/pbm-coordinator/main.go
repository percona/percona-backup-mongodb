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
	"github.com/percona/percona-backup-mongodb/grpc/api"
	"github.com/percona/percona-backup-mongodb/grpc/server"
	"github.com/percona/percona-backup-mongodb/internal/logger"
	apipb "github.com/percona/percona-backup-mongodb/proto/api"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
	yaml "gopkg.in/yaml.v2"

	_ "github.com/un000/grpc-snappy"
	_ "google.golang.org/grpc/encoding/gzip"
)

// vars are set by goreleaser
var (
	version = "dev"
	commit  = "none"
)

type cliOptions struct {
	app        *kingpin.Application
	cmd        string
	configFile string
	//
	WorkDir              string `yaml:"work_dir"`
	LogFile              string `yaml:"log_file"`
	Debug                bool   `yaml:"debug"`
	UseSysLog            bool   `yaml:"sys_log_url"`
	APIBindIP            string `yaml:"api_bindip"`
	APIPort              int    `yaml:"api_port"`
	GrpcBindIP           string `yaml:"grpc_bindip"`
	GrpcPort             int    `yaml:"grpc_port"`
	TLS                  bool   `yaml:"tls"`
	TLSCertFile          string `yaml:"tls_cert_file"`
	TLSKeyFile           string `yaml:"tls_key_file"`
	EnableClientsLogging bool   `yaml:"enable_clients_logging"`
	ShutdownTimeout      int    `yaml:"shutdown_timeout"`
}

const (
	defaultGrpcPort        = 10000
	defaultAPIPort         = 10001
	defaultShutdownTimeout = 5 // Seconds
	defaultClientsLogging  = true
	defaultDebugMode       = false
	defaultWorkDir         = "~/percona-backup-mongodb"
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

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", opts.GrpcBindIP, opts.GrpcPort))
	apilis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", opts.APIBindIP, opts.APIPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	var grpcOpts []grpc.ServerOption

	if opts.TLS {
		if opts.TLSCertFile == "" {
			opts.TLSCertFile = testdata.Path("server1.pem")
		}
		if opts.TLSKeyFile == "" {
			opts.TLSKeyFile = testdata.Path("server1.key")
		}
		creds, err := credentials.NewServerTLSFromFile(opts.TLSCertFile, opts.TLSKeyFile)
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
	app := kingpin.New("pbm-coordinator", "Percona Backup for MongoDB coordinator")
	app.Version(fmt.Sprintf("%s version %s, git commit %s", app.Name, version, commit))

	opts := &cliOptions{
		app: app,
	}

	app.Flag("config-file", "Config file").Default("config.yml").Short('c').StringVar(&opts.configFile)
	app.Flag("work-dir", "Working directory for backup metadata").Short('d').StringVar(&opts.WorkDir)
	app.Flag("log-file", "Write logs to file").Short('l').StringVar(&opts.LogFile)
	app.Flag("debug", "Enable debug log level").Short('v').BoolVar(&opts.Debug)
	app.Flag("use-syslog", "Also send the logs to the local syslog server").BoolVar(&opts.UseSysLog)
	//
	app.Flag("grpc-bindip", "Bind IP for gRPC client connections").StringVar(&opts.GrpcBindIP)
	app.Flag("grpc-port", "Listening port for gRPC client connections").IntVar(&opts.GrpcPort)
	app.Flag("api-bindip", "Bind IP for API client connections").StringVar(&opts.APIBindIP)
	app.Flag("api-port", "Listening port for API client connections").IntVar(&opts.APIPort)
	app.Flag("enable-clients-logging", "Enable showing logs comming from agents on the server side").BoolVar(&opts.EnableClientsLogging)
	app.Flag("shutdown-timeout", "Server shutdown timeout").IntVar(&opts.ShutdownTimeout)
	//
	app.Flag("tls", "Enable TLS").BoolVar(&opts.TLS)
	app.Flag("tls-cert-file", "Cert file for gRPC client connections").StringVar(&opts.TLSCertFile)
	app.Flag("tls-key-file", "Key file for gRPC client connections").StringVar(&opts.TLSKeyFile)

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
	if opts.TLSCertFile != "" {
		yamlOpts.TLSCertFile = opts.TLSCertFile
	}
	if opts.TLSKeyFile != "" {
		yamlOpts.TLSKeyFile = opts.TLSKeyFile
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
	opts.TLSCertFile = expandHomeDir(opts.TLSCertFile)
	opts.TLSKeyFile = expandHomeDir(opts.TLSKeyFile)
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
