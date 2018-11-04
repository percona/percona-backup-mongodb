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
	"github.com/percona/mongodb-backup/grpc/client"
	"github.com/percona/mongodb-backup/internal/logger"
	"github.com/percona/mongodb-backup/internal/loghook"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	yaml "gopkg.in/yaml.v2"
)

type cliOptions struct {
	app                  *kingpin.Application
	configFile           string
	generateSampleConfig bool

	BackupDir     string `yaml:"backup_dir"`
	CAFile        string `yaml:"ca_file"`
	CertFile      string `yaml:"cert_file"`
	DSN           string `yaml:"dsn"`
	Debug         bool   `yaml:"debug"`
	KeyFile       string `yaml:"key_file"`
	LogFile       string `yaml:"log_file"`
	PIDFile       string `yaml:"pid_file"`
	Quiet         bool   `yaml:"quiet"`
	ServerAddress string `yaml:"server_address"`
	TLS           bool   `yaml:"tls"`
	UseSysLog     bool   `yaml:"use_syslog"`

	// MongoDB connection options
	MongodbConnOptions client.ConnectionOptions `yaml:"mongodb_conn_options"`

	// MongoDB connection SSL options
	MongodbSslOptions client.SSLOptions `yaml:"mongodb_ssl_options"`
}

const (
	sampleConfigFile = "config.sample.yml"
)

var (
	log = logrus.New()
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
	rand.Seed(time.Now().UnixNano())
	clientID := fmt.Sprintf("ABC%s", opts.MongodbConnOptions.Port)
	log.Infof("Using Client ID: %s", clientID)

	// Connect to the mongodb-backup gRPC server
	conn, err := grpc.Dial(opts.ServerAddress, grpcOpts...)
	if err != nil {
		log.Fatalf("Fail to connect to the gRPC server at %q: %v", opts.ServerAddress, err)
	}
	defer conn.Close()
	log.Infof("Connected to the gRPC server at %s", opts.ServerAddress)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logHook, err := loghook.NewGrpcLogging(ctx, clientID, conn)
	logHook.SetLevel(log.Level)
	log.AddHook(logHook)

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

	mdbSession, err := mgo.DialWithInfo(di)
	if err != nil {
		log.Fatalf("Cannot connect to MongoDB at %s: %s", di.Addrs[0], err)
	}
	defer mdbSession.Close()
	log.Infof("Connected to MongoDB at %s", di.Addrs[0])

	client, err := client.NewClient(context.Background(), opts.BackupDir, opts.MongodbConnOptions, opts.MongodbSslOptions, conn, log)
	if err != nil {
		log.Fatal(err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	<-c
	client.Stop()
}

func processCliArgs() (*cliOptions, error) {
	app := kingpin.New("pmb-admin", "MongoDB backup admin client")
	opts := &cliOptions{
		app: app,
	}
	app.Flag("config-file", "Config file name").Default("config.yml").StringVar(&opts.configFile)
	app.Flag("generate-sample-config", "Generate sample config.yml file with the defaults").BoolVar(&opts.generateSampleConfig)
	//
	app.Flag("dsn", "MongoDB connection string").StringVar(&opts.DSN)
	app.Flag("server-address", "Backup server address").Default("127.0.0.1:10000").StringVar(&opts.ServerAddress)
	app.Flag("backup-dir", "Directory where to store the backups").Default("/tmp").StringVar(&opts.BackupDir)
	app.Flag("tls", "Use TLS").BoolVar(&opts.TLS)
	app.Flag("pid-file", "pid file").StringVar(&opts.PIDFile)
	app.Flag("ca-file", "CA file").StringVar(&opts.CAFile)
	app.Flag("cert-file", "Cert file").StringVar(&opts.CertFile)
	app.Flag("key-file", "Key file").StringVar(&opts.KeyFile)
	app.Flag("debug", "Enable debug log level").BoolVar(&opts.Debug)
	app.Flag("quiet", "Quiet mode. Log only errors").BoolVar(&opts.Quiet)
	app.Flag("log-file", "Log file").StringVar(&opts.LogFile)
	app.Flag("use-syslog", "Use syslog instead of Stderr or file").BoolVar(&opts.UseSysLog)

	app.Flag("mongodb-host", "MongoDB host").StringVar(&opts.MongodbConnOptions.Host)
	app.Flag("mongodb-port", "MongoDB port").StringVar(&opts.MongodbConnOptions.Port)
	app.Flag("mongodb-user", "MongoDB username").StringVar(&opts.MongodbConnOptions.User)
	app.Flag("mongodb-password", "MongoDB password").StringVar(&opts.MongodbConnOptions.Password)
	app.Flag("replicaset", "Replicaset name").StringVar(&opts.MongodbConnOptions.ReplicasetName)

	_, err := app.Parse(os.Args[1:])
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
	if opts.CAFile != "" {
		yamlOpts.CAFile = opts.CAFile
	}
	if opts.CertFile != "" {
		yamlOpts.CertFile = opts.CertFile
	}
	if opts.KeyFile != "" {
		yamlOpts.KeyFile = opts.KeyFile
	}
	if opts.Debug != false {
		yamlOpts.Debug = opts.Debug
	}
	if opts.generateSampleConfig != false {
		yamlOpts.generateSampleConfig = opts.generateSampleConfig
	}
	if opts.DSN != "" {
		yamlOpts.DSN = opts.DSN
	}
}

func expandDirs(opts *cliOptions) {
	opts.BackupDir = expandHomeDir(opts.BackupDir)
	opts.CAFile = expandHomeDir(opts.CAFile)
	opts.CertFile = expandHomeDir(opts.CertFile)
	opts.KeyFile = expandHomeDir(opts.KeyFile)
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
		creds, err := credentials.NewClientTLSFromFile(opts.CAFile, "")
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
		Level: logrus.InfoLevel,
	}
	logger.SetLevel(logrus.StandardLogger().Level)
	logger.Out = logrus.StandardLogger().Out

	return logger
}
