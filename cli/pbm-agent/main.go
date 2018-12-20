package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/globalsign/mgo"
	"github.com/percona/percona-backup-mongodb/grpc/client"
	"github.com/percona/percona-backup-mongodb/internal/logger"
	"github.com/percona/percona-backup-mongodb/internal/loghook"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	snappy "github.com/un000/grpc-snappy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding/gzip"
	yaml "gopkg.in/yaml.v2"
)

// vars are set by goreleaser
var (
	version = "dev"
	commit  = "none"
)

type cliOptions struct {
	app                  *kingpin.Application
	configFile           string
	generateSampleConfig bool

	BackupDir             string `yaml:"backup_dir"`
	DSN                   string `yaml:"dsn"`
	Debug                 bool   `yaml:"debug"`
	LogFile               string `yaml:"log_file"`
	PIDFile               string `yaml:"pid_file"`
	Quiet                 bool   `yaml:"quiet"`
	ServerAddress         string `yaml:"server_address"`
	ServerCompressor      string `yaml:"server_compressor"`
	TLS                   bool   `yaml:"tls"`
	TLSCAFile             string `yaml:"tls_ca_file"`
	TLSCertFile           string `yaml:"tls_cert_file"`
	TLSKeyFile            string `yaml:"tls_key_file"`
	UseSysLog             bool   `yaml:"use_syslog"`
	MongoDBReconnectDelay int    `yaml:"mongodb_reconnect_delay"`
	MongoDBReconnectCount int    `yaml:"mongodb_reconnect_count"` // 0: forever

	// MongoDB connection options
	MongodbConnOptions client.ConnectionOptions `yaml:"mongodb_conn_options"`

	// MongoDB connection SSL options
	MongodbSslOptions client.SSLOptions `yaml:"mongodb_ssl_options"`
}

const (
	sampleConfigFile     = "config.sample.yml"
	defaultServerAddress = "127.0.0.1:10000"
	defaultMongoDBHost   = "127.0.0.1"
	defaultMongoDBPort   = "27017"
)

var (
	log             = logrus.New()
	grpcCompressors = []string{
		snappy.Name,
		gzip.Name,
		"none",
	}
)

func main() {
	opts, err := processCliArgs()
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

	if opts.generateSampleConfig {
		if err := writeSampleConfig(sampleConfigFile, opts); err != nil {
			log.Fatalf("Cannot write sample config file %s: %s", sampleConfigFile, err)
		}
		log.Printf("Sample config was written to %s\n", sampleConfigFile)
		return
	}

	if opts.Quiet {
		log.SetLevel(logrus.ErrorLevel)
	}
	if opts.Debug {
		log.SetLevel(logrus.DebugLevel)
	}
	log.SetLevel(logrus.DebugLevel)

	grpcOpts := getgRPCOptions(opts)

	if opts.ServerCompressor != "" && opts.ServerCompressor != "none" {
		grpcOpts = append(grpcOpts, grpc.WithDefaultCallOptions(
			grpc.UseCompressor(opts.ServerCompressor),
		))
	}

	rand.Seed(time.Now().UnixNano())

	// Connect to the percona-backup-mongodb gRPC server
	conn, err := grpc.Dial(opts.ServerAddress, grpcOpts...)
	if err != nil {
		log.Fatalf("Fail to connect to the gRPC server at %q: %v", opts.ServerAddress, err)
	}
	defer conn.Close()
	log.Infof("Connected to the gRPC server at %s", opts.ServerAddress)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to the MongoDB instance
	var di *mgo.DialInfo
	if opts.DSN == "" {
		di = &mgo.DialInfo{
			Addrs:          []string{opts.MongodbConnOptions.Host + ":" + opts.MongodbConnOptions.Port},
			Username:       opts.MongodbConnOptions.User,
			Password:       opts.MongodbConnOptions.Password,
			ReplicaSetName: opts.MongodbConnOptions.ReplicasetName,
			FailFast:       true,
			Source:         "admin",
		}
	} else {
		di, err = mgo.ParseURL(opts.DSN)
		di.FailFast = true
		if err != nil {
			log.Fatalf("Cannot parse MongoDB DSN %q, %s", opts.DSN, err)
		}
	}
	mdbSession := &mgo.Session{}
	connectionAttempts := 0
	for {
		connectionAttempts++
		mdbSession, err = mgo.DialWithInfo(di)
		if err != nil {
			log.Errorf("Cannot connect to MongoDB at %s: %s", di.Addrs[0], err)
			if opts.MongoDBReconnectCount == 0 || connectionAttempts < opts.MongoDBReconnectCount {
				time.Sleep(time.Duration(opts.MongoDBReconnectDelay) * time.Second)
				continue
			}
			log.Fatalf("Could not connect to MongoDB. Retried every %d seconds, %d times", opts.MongoDBReconnectDelay, connectionAttempts)
		}
		break
	}

	log.Infof("Connected to MongoDB at %s", di.Addrs[0])
	defer mdbSession.Close()

	client, err := client.NewClient(context.Background(), opts.BackupDir, opts.MongodbConnOptions, opts.MongodbSslOptions, conn, log)
	if err != nil {
		log.Fatal(err)
	}

	logHook, err := loghook.NewGrpcLogging(ctx, client.ID(), conn)
	if err != nil {
		log.Fatalf("Failed to create gRPC log hook: %v", err)
	}
	logHook.SetLevel(log.Level)
	log.AddHook(logHook)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	<-c
	client.Stop()
}

func processCliArgs() (*cliOptions, error) {
	app := kingpin.New("pbm-agent", "Percona Backup for MongoDB agent")
	app.Version(fmt.Sprintf("%s version %s, git commit %s", app.Name, version, commit))

	opts := &cliOptions{
		app: app,
	}
	app.Flag("config-file", "Backup Agent config file").Default("config.yml").ExistingFileVar(&opts.configFile)
	app.Flag("generate-sample-config", "Generate sample config.yml file with the defaults").BoolVar(&opts.generateSampleConfig)
	app.Flag("backup-dir", "Directory to store backups").Default("/tmp").StringVar(&opts.BackupDir)
	app.Flag("pid-file", "Backup Agent pid file").StringVar(&opts.PIDFile)
	app.Flag("log-file", "Backup Agent log file").StringVar(&opts.LogFile)
	app.Flag("use-syslog", "Use syslog instead of Stderr or file").BoolVar(&opts.UseSysLog)
	app.Flag("debug", "Enable debug log level").BoolVar(&opts.Debug)
	app.Flag("quiet", "Quiet mode. Log only errors").BoolVar(&opts.Quiet)
	//
	app.Flag("server-address", "Backup server address (host:port)").Default(defaultServerAddress).StringVar(&opts.ServerAddress)
	app.Flag("server-compressor", "Backup server gRPC compression algorithm").Default(snappy.Name).EnumVar(&opts.ServerCompressor, grpcCompressors...)
	app.Flag("tls", "Use TLS for server connection").BoolVar(&opts.TLS)
	app.Flag("tls-cert-file", "TLS certificate file").ExistingFileVar(&opts.TLSCertFile)
	app.Flag("tls-key-file", "TLS key file").ExistingFileVar(&opts.TLSKeyFile)
	app.Flag("tls-ca-file", "TLS CA file").ExistingFileVar(&opts.TLSCAFile)
	//
	app.Flag("mongodb-dsn", "MongoDB connection string").StringVar(&opts.DSN)
	app.Flag("mongodb-host", "MongoDB hostname").Default(defaultMongoDBHost).StringVar(&opts.MongodbConnOptions.Host)
	app.Flag("mongodb-port", "MongoDB port").Default(defaultMongoDBPort).StringVar(&opts.MongodbConnOptions.Port)
	app.Flag("mongodb-username", "MongoDB username").StringVar(&opts.MongodbConnOptions.User)
	app.Flag("mongodb-password", "MongoDB password").StringVar(&opts.MongodbConnOptions.Password)
	app.Flag("mongodb-replicaset", "MongoDB Replicaset name").StringVar(&opts.MongodbConnOptions.ReplicasetName)
	app.Flag("mongodb-reconnect-delay", "MongoDB reconnection delay in seconds").Default("30").IntVar(&opts.MongoDBReconnectDelay)
	app.Flag("mongodb-reconnect-count", "MongoDB max reconnection attempts (0: forever)").IntVar(&opts.MongoDBReconnectCount)

	_, err := app.DefaultEnvars().Parse(os.Args[1:])
	if err != nil {
		return nil, err
	}

	//TODO: Remove defaults. These values are only here to test during development
	// These params should be Required()
	yamlOpts := &cliOptions{}

	if opts.configFile != "" {
		loadOptionsFromFile(opts.configFile, yamlOpts)
	}

	mergeOptions(opts, yamlOpts)
	validateOptions(yamlOpts)
	return opts, nil
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

func validateOptions(opts *cliOptions) error {
	if opts.PIDFile != "" {
		if err := writePidFile(opts.PIDFile); err != nil {
			return errors.Wrapf(err, "cannot write pid file %q", opts.PIDFile)
		}
	}

	if opts.DSN != "" {
		di, err := mgo.ParseURL(opts.DSN)
		if err != nil {
			return err
		}
		parts := strings.Split(di.Addrs[0], ":")
		if len(parts) < 2 {
			return fmt.Errorf("invalid host:port: %s", di.Addrs[0])
		}
		opts.MongodbConnOptions.Host = parts[0]
		opts.MongodbConnOptions.Port = parts[1]
		opts.MongodbConnOptions.User = di.Username
		opts.MongodbConnOptions.Password = di.Password
		opts.MongodbConnOptions.ReplicasetName = di.ReplicaSetName
	}

	fi, err := os.Stat(opts.BackupDir)
	if err != nil {
		return errors.Wrap(err, "invalid backup destination dir")
	}
	if !fi.IsDir() {
		return fmt.Errorf("%q is not a directory", opts.BackupDir)
	}

	return nil
}

// This function takes yamlOpts as the default values and then, if the user has specified different
// values for some options, they will be overwitten.
func mergeOptions(opts, yamlOpts *cliOptions) {
	if opts.TLS != false {
		yamlOpts.TLS = opts.TLS
	}
	if opts.BackupDir != "" {
		yamlOpts.BackupDir = opts.BackupDir
	}
	if opts.TLSCAFile != "" {
		yamlOpts.TLSCAFile = opts.TLSCAFile
	}
	if opts.TLSCertFile != "" {
		yamlOpts.TLSCertFile = opts.TLSCertFile
	}
	if opts.TLSKeyFile != "" {
		yamlOpts.TLSKeyFile = opts.TLSKeyFile
	}
	if opts.Debug != false {
		yamlOpts.Debug = opts.Debug
	}
	if opts.ServerCompressor != "" {
		yamlOpts.ServerCompressor = opts.ServerCompressor
	}
	if opts.generateSampleConfig != false {
		yamlOpts.generateSampleConfig = opts.generateSampleConfig
	}
	if opts.DSN != "" {
		yamlOpts.DSN = opts.DSN
	}
	if opts.MongoDBReconnectDelay != 0 {
		yamlOpts.MongoDBReconnectDelay = opts.MongoDBReconnectDelay
	}
	if opts.MongoDBReconnectCount != 0 {
		yamlOpts.MongoDBReconnectCount = opts.MongoDBReconnectCount
	}
	if opts.MongodbConnOptions.Host != "" {
		yamlOpts.MongodbConnOptions.Host = opts.MongodbConnOptions.Host
	}
	if opts.MongodbConnOptions.Port != "" {
		yamlOpts.MongodbConnOptions.Port = opts.MongodbConnOptions.Port
	}
}

func expandDirs(opts *cliOptions) {
	opts.BackupDir = expandHomeDir(opts.BackupDir)
	opts.TLSCAFile = expandHomeDir(opts.TLSCAFile)
	opts.TLSCertFile = expandHomeDir(opts.TLSCertFile)
	opts.TLSKeyFile = expandHomeDir(opts.TLSKeyFile)
	opts.PIDFile = expandHomeDir(opts.PIDFile)
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

func writeSampleConfig(filename string, opts *cliOptions) error {
	buf, err := yaml.Marshal(opts)
	if err != nil {
		return err
	}
	if err = ioutil.WriteFile(filename, buf, os.ModePerm); err != nil {
		return errors.Wrapf(err, "cannot write sample config to %s", filename)
	}
	return nil
}

func getgRPCOptions(opts *cliOptions) []grpc.DialOption {
	var grpcOpts []grpc.DialOption
	if opts.TLS {
		creds, err := credentials.NewClientTLSFromFile(opts.TLSCAFile, "")
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

func getDefaultLogger() *logrus.Logger {
	logger := &logrus.Logger{
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

	return logger
}
