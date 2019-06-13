package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/signal"
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
	"github.com/percona/percona-backup-mongodb/internal/utils"
	"github.com/percona/percona-backup-mongodb/storage"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding/gzip"
	yaml "gopkg.in/yaml.v2"
)

const (
	defaultReconnectCount = 3
	defaultReconnectDelay = 10
	sampleConfigFile      = "config.sample.yml"
	defaultServerAddress  = "127.0.0.1:10000"
	defaultMongoDBHost    = "127.0.0.1"
	defaultMongoDBPort    = "27017"
)

type dialerFn func(addr *mgo.ServerAddr) (net.Conn, error)

type cliOptions struct {
	app                  *kingpin.Application
	configFile           string
	generateSampleConfig bool

	Debug            bool   `yaml:"debug,omitempty" kingpin:"debug"`
	LogFile          string `yaml:"log_file,omitempty" kingpin:"log-file"`
	PIDFile          string `yaml:"pid_file,omitempty" kingpin:"pid-file"`
	Quiet            bool   `yaml:"quiet,omitempty" kingpin:"quiet"`
	ServerAddress    string `yaml:"server_address" kingping:"server-address"`
	ServerCompressor string `yaml:"server_compressor" kingpin:"server-compressor"`
	StoragesConfig   string `yaml:"storage_config" kingpin:"storage-config"`
	TLS              bool   `yaml:"tls,omitempty" kingpin:"tls"`
	TLSCAFile        string `yaml:"tls_ca_file,omitempty" kingpin:"tls-ca-file"`
	TLSCertFile      string `yaml:"tls_cert_file,omitempty" kingpin:"tls-cert-file"`
	TLSKeyFile       string `yaml:"tls_key_file,omitempty" kingpin:"tls-key-file"`

	UseSysLog bool `yaml:"use_syslog,omitempty" kingpin:"use-syslog"`

	// MongoDB connection options
	MongodbConnOptions client.ConnectionOptions `yaml:"mongodb_conn_options,omitempty"`

	// MongoDB connection SSL options
	MongodbSslOptions client.SSLOptions `yaml:"mongodb_ssl_options,omitempty"`
}

var (
	Version         = "dev"
	Commit          = "none"
	Build           = "date"
	Branch          = "master"
	GoVersion       = "0.0.0"
	log             = logrus.New()
	program         = filepath.Base(os.Args[0])
	grpcCompressors = []string{
		gzip.Name,
		"none",
	}
)

func main() {
	opts, err := processCliArgs(os.Args[1:])
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
	if opts.Debug || os.Getenv("DEBUG") == "1" {
		log.SetLevel(logrus.DebugLevel)
	}

	log.Infof("Starting %s version %s, git commit %s", program, Version, Commit)

	grpcOpts := getgRPCOptions(opts)

	// Connect to the percona-backup-mongodb gRPC server
	conn, err := grpc.Dial(opts.ServerAddress, grpcOpts...)
	if err != nil {
		log.Fatalf("Fail to connect to the gRPC server at %q: %v", opts.ServerAddress, err)
	}

	defer conn.Close()
	log.Infof("Connected to the gRPC server at %s", opts.ServerAddress)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	di, err := buildDialInfo(opts)
	if err != nil {
		log.Fatalf("Cannot parse MongoDB DSN %q, %s", opts.MongodbConnOptions.DSN, err)
	}

	// Test the connection to the MongoDB server before starting the agent.
	// We don't want to wait until backup/restore start to know there is an error with the
	// connection options
	mdbSession, err := dbConnect(di, opts.MongodbConnOptions.ReconnectCount, opts.MongodbConnOptions.ReconnectDelay)
	if err != nil {
		log.Fatalf("Cannot connect to the database: %s", err)
	}
	mdbSession.Close()

	log.Infof("Connected to MongoDB at %s", di.Addrs[0])

	stg, err := storage.NewStorageBackendsFromYaml(utils.Expand(opts.StoragesConfig))
	if err != nil {
		log.Fatalf("Canot load storage config from file %s: %s", utils.Expand(opts.StoragesConfig), err)
	}

	input := client.InputOptions{
		DbConnOptions: opts.MongodbConnOptions,
		DbSSLOptions:  opts.MongodbSslOptions,
		GrpcConn:      conn,
		Logger:        log,
		Storages:      stg,
	}

	client, err := client.NewClient(context.Background(), input)
	if err != nil {
		log.Fatal(err)
	}

	stgs, err := client.ListStorages()
	if err != nil {
		log.Fatalf("Cannot check if all storage entries in file %q are valid: %s", opts.StoragesConfig, err)
	}
	count := 0
	for _, stg := range stgs {
		if stg.Valid {
			count++
			continue
		}
		log.Errorf("Config for storage %s is not valid. Can read: %v; Can write: %v", stg.Name, stg.CanRead, stg.CanWrite)
	}
	if count != len(stgs) { // not all storage entries are valid
		log.Error("Not all storage config entries are valid")
	}

	if err := client.Start(); err != nil {
		log.Fatalf("Cannot start client: %s", err)
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
	if err := client.Stop(); err != nil {
		log.Fatalf("Cannot stop client: %s", err)
	}
}

func processCliArgs(args []string) (*cliOptions, error) {
	app := kingpin.New("pbm-agent", "Percona Backup for MongoDB agent")
	app.Version(versionMessage())

	opts := &cliOptions{
		app:           app,
		ServerAddress: defaultServerAddress,
		MongodbConnOptions: client.ConnectionOptions{
			Host: defaultMongoDBHost,
			Port: defaultMongoDBPort,
		},
	}

	app.Flag("config-file", "Backup agent config file").
		Short('c').
		StringVar(&opts.configFile)
	app.Flag("debug", "Enable debug log level").Short('v').
		BoolVar(&opts.Debug)
	app.Flag("generate-sample-config", "Generate sample config.yml file with the defaults").
		BoolVar(&opts.generateSampleConfig)
	app.Flag("log-file", "Backup agent log file").
		Short('l').
		StringVar(&opts.LogFile)
	app.Flag("pid-file", "Backup agent pid file").
		StringVar(&opts.PIDFile)
	app.Flag("quiet", "Quiet mode. Log only errors").
		Short('q').
		BoolVar(&opts.Quiet)
	app.Flag("storage-config", "Storage config yaml file").
		Required().
		StringVar(&opts.StoragesConfig)
	app.Flag("use-syslog", "Use syslog instead of Stderr or file").
		BoolVar(&opts.UseSysLog)
	app.Flag("server-address", "Backup coordinator address (host:port)").
		Short('s').
		StringVar(&opts.ServerAddress)
	app.Flag("server-compressor", "Backup coordintor gRPC compression (gzip or none)").
		Default().
		EnumVar(&opts.ServerCompressor, grpcCompressors...)

	app.Flag("tls", "Use TLS for server connection").
		BoolVar(&opts.TLS)
	app.Flag("tls-cert-file", "TLS certificate file").
		ExistingFileVar(&opts.TLSCertFile)
	app.Flag("tls-key-file", "TLS key file").
		ExistingFileVar(&opts.TLSKeyFile)
	app.Flag("tls-ca-file", "TLS CA file").
		ExistingFileVar(&opts.TLSCAFile)

	app.Flag("mongodb-ssl-pem-key-file", "MongoDB SSL pem file").
		ExistingFileVar(&opts.MongodbSslOptions.SSLPEMKeyFile)
	app.Flag("mongodb-ssl-ca-file", "MongoDB SSL CA file").
		ExistingFileVar(&opts.MongodbSslOptions.SSLCAFile)
	app.Flag("mongodb-ssl-pem-key-password", "MongoDB SSL key file password").
		StringVar(&opts.MongodbSslOptions.SSLPEMKeyPassword)

	app.Flag("mongodb-dsn", "MongoDB connection string").
		StringVar(&opts.MongodbConnOptions.DSN)
	app.Flag("mongodb-host", "MongoDB hostname").
		Short('H').
		StringVar(&opts.MongodbConnOptions.Host)
	app.Flag("mongodb-port", "MongoDB port").
		Short('P').
		StringVar(&opts.MongodbConnOptions.Port)
	app.Flag("mongodb-username", "MongoDB username").
		Short('u').
		StringVar(&opts.MongodbConnOptions.User)
	app.Flag("mongodb-password", "MongoDB password").
		Short('p').
		StringVar(&opts.MongodbConnOptions.Password)
	app.Flag("mongodb-authdb", "MongoDB authentication database").
		StringVar(&opts.MongodbConnOptions.AuthDB)
	app.Flag("mongodb-replicaset", "MongoDB Replicaset name").
		StringVar(&opts.MongodbConnOptions.ReplicasetName)
	app.Flag("mongodb-reconnect-delay", "MongoDB reconnection delay in seconds").
		Default(fmt.Sprintf("%d", defaultReconnectDelay)).
		IntVar(&opts.MongodbConnOptions.ReconnectDelay)
	app.Flag("mongodb-reconnect-count", "MongoDB max reconnection attempts (0: forever)").
		Default(fmt.Sprintf("%d", defaultReconnectCount)).
		IntVar(&opts.MongodbConnOptions.ReconnectCount)

	app.PreAction(func(c *kingpin.ParseContext) error {
		if opts.configFile == "" {
			fn := utils.Expand("~/.percona-backup-mongodb.yaml")
			if _, err := os.Stat(fn); err != nil {
				return nil
			}
			opts.configFile = fn
		}
		return utils.LoadOptionsFromFile(opts.configFile, c, opts)
	})

	_, err := app.DefaultEnvars().Parse(args)
	if err != nil {
		return nil, err
	}

	if err := validateOptions(opts); err != nil {
		return nil, err
	}
	return opts, nil
}

func validateOptions(opts *cliOptions) error {
	opts.TLSCAFile = utils.Expand(opts.TLSCAFile)
	opts.TLSCertFile = utils.Expand(opts.TLSCertFile)
	opts.TLSKeyFile = utils.Expand(opts.TLSKeyFile)
	opts.PIDFile = utils.Expand(opts.PIDFile)

	opts.MongodbSslOptions.SSLCAFile = utils.Expand(opts.MongodbSslOptions.SSLCAFile)
	opts.MongodbSslOptions.SSLCRLFile = utils.Expand(opts.MongodbSslOptions.SSLCRLFile)
	opts.MongodbSslOptions.SSLPEMKeyFile = utils.Expand(opts.MongodbSslOptions.SSLPEMKeyFile)

	if opts.PIDFile != "" {
		if err := writePidFile(opts.PIDFile); err != nil {
			return errors.Wrapf(err, "cannot write pid file %q", opts.PIDFile)
		}
	}

	if opts.MongodbConnOptions.DSN != "" {
		di, err := mgo.ParseURL(opts.MongodbConnOptions.DSN)
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

	return nil
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
	if opts.ServerCompressor != "" && opts.ServerCompressor != "none" {
		grpcOpts = append(grpcOpts, grpc.WithDefaultCallOptions(
			grpc.UseCompressor(opts.ServerCompressor),
		))
	}
	return grpcOpts
}

func makeDialer(caFile, keyFile, keyPassword string) (dialerFn, error) {
	// --sslCAFile
	rootCerts := x509.NewCertPool()
	ca, err := ioutil.ReadFile(caFile)
	if err != nil {
		return nil, errors.Wrap(err, "cannot read CA cert file")
	}
	rootCerts.AppendCertsFromPEM(ca)

	// --sslPEMKeyFile
	clientCerts := []tls.Certificate{}

	key, err := ioutil.ReadFile(keyFile)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot read %s", keyFile)
	}

	keyblock, crt := decodeKey(key, keyPassword)

	cert, err := tls.X509KeyPair(crt, keyblock)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot read cert or key file")
	}

	clientCerts = append(clientCerts, cert)

	dialer := func(addr *mgo.ServerAddr) (net.Conn, error) {
		return tls.Dial("tcp", addr.String(), &tls.Config{
			RootCAs:      rootCerts,
			Certificates: clientCerts,
		})
	}

	return dialer, nil
}

func decodeKey(key []byte, pass string) ([]byte, []byte) {
	var v *pem.Block
	var pkey []byte
	var pemBlocks []*pem.Block

	for {
		v, key = pem.Decode(key)
		if v == nil {
			break
		}
		if v.Type == "RSA PRIVATE KEY" {
			if x509.IsEncryptedPEMBlock(v) {
				pkey, _ = x509.DecryptPEMBlock(v, []byte(pass))
				pkey = pem.EncodeToMemory(&pem.Block{
					Type:  v.Type,
					Bytes: pkey,
				})
			} else {
				pkey = pem.EncodeToMemory(v)
			}
		} else {
			pemBlocks = append(pemBlocks, v)
		}
	}
	return pkey, pem.EncodeToMemory(pemBlocks[0])
}

// Write a pid file, but first make sure it doesn't exist with a running pid.
func writePidFile(pidFile string) error {
	// Read in the pid file as a slice of bytes.
	if piddata, err := ioutil.ReadFile(filepath.Clean(pidFile)); err == nil {
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

func versionMessage() string {
	msg := "Version   : " + Version + "\n"
	msg += "Commit    : " + Commit + "\n"
	msg += "Build     : " + Build + "\n"
	msg += "Branch    : " + Branch + "\n"
	msg += "Go version: " + GoVersion + "\n"
	return msg
}

func buildDialInfo(opts *cliOptions) (*mgo.DialInfo, error) {
	var di *mgo.DialInfo
	var err error

	if opts.MongodbConnOptions.DSN == "" {
		di = &mgo.DialInfo{
			Addrs:          []string{opts.MongodbConnOptions.Host + ":" + opts.MongodbConnOptions.Port},
			Username:       opts.MongodbConnOptions.User,
			Password:       opts.MongodbConnOptions.Password,
			ReplicaSetName: opts.MongodbConnOptions.ReplicasetName,
			FailFast:       true,
			Source:         "admin",
		}
	} else {
		di, err = mgo.ParseURL(opts.MongodbConnOptions.DSN)
		di.FailFast = true
		if err != nil {
			return nil, errors.Wrapf(err, "Cannot parse MongoDB DSN %q", opts.MongodbConnOptions.DSN)
		}
	}

	if opts.MongodbSslOptions.SSLCAFile != "" && opts.MongodbSslOptions.SSLPEMKeyFile != "" {
		dialer, err := makeDialer(opts.MongodbSslOptions.SSLCAFile,
			opts.MongodbSslOptions.SSLPEMKeyFile, opts.MongodbSslOptions.SSLPEMKeyPassword)
		if err != nil {
			log.Fatalf("cannot create a MongoDB dialer: %s", err)
		}
		di.DialServer = dialer
		opts.MongodbSslOptions.UseSSL = true
	}

	return di, nil
}

func dbConnect(di *mgo.DialInfo, maxReconnects int, reconnectDelay int) (*mgo.Session, error) {
	connectionAttempts := 0
	var session *mgo.Session
	var err error

	for {
		connectionAttempts++
		session, err = mgo.DialWithInfo(di)
		if err != nil {
			log.Errorf("Cannot connect to MongoDB at %s: %s. Connection atempt %d of %d",
				di.Addrs[0], err, connectionAttempts, maxReconnects)
			if maxReconnects == 0 || connectionAttempts < maxReconnects {
				time.Sleep(time.Duration(reconnectDelay) * time.Second)
				continue
			}
			return nil, fmt.Errorf("could not connect to MongoDB. Retried every %d seconds, %d times",
				reconnectDelay, connectionAttempts)
		}

		if err := session.Ping(); err != nil {
			return nil, errors.Wrap(err, "Cannot connect to the database server")
		}
		break
	}

	return session, nil
}
