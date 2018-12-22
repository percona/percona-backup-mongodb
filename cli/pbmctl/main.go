package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"sort"

	"text/template"

	"github.com/alecthomas/kingpin"
	"github.com/percona/percona-backup-mongodb/internal/templates"
	pbapi "github.com/percona/percona-backup-mongodb/proto/api"
	pb "github.com/percona/percona-backup-mongodb/proto/messages"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	snappy "github.com/un000/grpc-snappy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/testdata"
	yaml "gopkg.in/yaml.v2"
)

// vars are set by goreleaser
var (
	version = "dev"
	commit  = "none"
)

type cliOptions struct {
	app *kingpin.Application

	TLS              bool   `yaml:"tls"`
	TLSCAFile        string `yaml:"tls_ca_file"`
	ServerAddr       string `yaml:"server_addr"`
	ServerCompressor string `yaml:"server_compressor"`
	configFile       *string

	backup               *kingpin.CmdClause
	backupType           *string
	destinationType      *string
	compressionAlgorithm *string
	encryptionAlgorithm  *string
	description          *string

	restore                  *kingpin.CmdClause
	restoreMetadataFile      *string
	restoreSkipUsersAndRoles *bool

	list             *kingpin.CmdClause
	listNodes        *kingpin.CmdClause
	listNodesVerbose *bool
	listBackups      *kingpin.CmdClause
}

var (
	conn              *grpc.ClientConn
	defaultServerAddr = "127.0.0.1:10001"
	defaultConfigFile = "~/.pbmctl.yml"
	grpcCompressors   = []string{
		snappy.Name,
		gzip.Name,
		"none",
	}
)

func main() {
	cmd, opts, err := processCliArgs(os.Args[1:])
	if err != nil {
		if opts != nil {
			kingpin.Usage()
		}
		log.Fatal(err)
	}

	var grpcOpts []grpc.DialOption
	if opts.ServerCompressor != "" && opts.ServerCompressor != "none" {
		grpcOpts = append(grpcOpts, grpc.WithDefaultCallOptions(
			grpc.UseCompressor(opts.ServerCompressor),
		))
	}

	if opts.TLS {
		if opts.TLSCAFile == "" {
			opts.TLSCAFile = testdata.Path("ca.pem")
		}
		creds, err := credentials.NewClientTLSFromFile(opts.TLSCAFile, "")
		if err != nil {
			log.Fatalf("Failed to create TLS credentials %v", err)
		}
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(creds))
	} else {
		grpcOpts = append(grpcOpts, grpc.WithInsecure())
	}

	ctx, cancel := context.WithCancel(context.Background())

	conn, err = grpc.Dial(opts.ServerAddr, grpcOpts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	apiClient := pbapi.NewApiClient(conn)
	if err != nil {
		log.Fatalf("Cannot connect to the API: %s", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		<-c
		cancel()
	}()

	switch cmd {
	case "list nodes":
		clients, err := connectedAgents(ctx, conn)
		if err != nil {
			log.Errorf("Cannot get the list of connected agents: %s", err)
			break
		}
		if *opts.listNodesVerbose {
			printTemplate(templates.ConnectedNodesVerbose, clients)
		} else {
			printTemplate(templates.ConnectedNodes, clients)
		}
	case "list backups":
		md, err := getAvailableBackups(ctx, conn)
		if err != nil {
			log.Errorf("Cannot get the list of available backups: %s", err)
			break
		}
		if len(md) > 0 {
			printTemplate(templates.AvailableBackups, md)
			return
		}
		fmt.Println("No backups found")
	case "run backup":
		err := startBackup(ctx, apiClient, opts)
		if err != nil {
			log.Fatal(err)
			log.Fatalf("Cannot send the StartBackup command to the gRPC server: %s", err)
		}
		log.Println("Backup completed")
	case "run restore":
		fmt.Println("restoring")
		err := restoreBackup(ctx, apiClient, opts)
		if err != nil {
			log.Fatal(err)
			log.Fatalf("Cannot send the RestoreBackup command to the gRPC server: %s", err)
		}
		log.Println("Restore completed")
	default:
		log.Fatalf("Unknown command %q", cmd)
	}

	cancel()
}

func connectedAgents(ctx context.Context, conn *grpc.ClientConn) ([]*pbapi.Client, error) {
	apiClient := pbapi.NewApiClient(conn)
	stream, err := apiClient.GetClients(ctx, &pbapi.Empty{})
	if err != nil {
		return nil, err
	}
	clients := []*pbapi.Client{}
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Wrap(err, "Cannot get the connected agents list")
		}
		clients = append(clients, msg)
	}
	sort.Slice(clients, func(i, j int) bool { return clients[i].NodeName < clients[j].NodeName })
	return clients, nil
}

func getAvailableBackups(ctx context.Context, conn *grpc.ClientConn) (map[string]*pb.BackupMetadata, error) {
	apiClient := pbapi.NewApiClient(conn)
	stream, err := apiClient.BackupsMetadata(ctx, &pbapi.BackupsMetadataParams{})
	if err != nil {
		return nil, err
	}

	mds := make(map[string]*pb.BackupMetadata)
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Wrap(err, "Cannot get the connected agents list")
		}
		mds[msg.Filename] = msg.Metadata
	}

	return mds, nil
}

// This function is used by autocompletion. Currently, when it is called, the gRPC connection is nil
// because command line parameters havent been processed yet.
// Maybe in the future, we could read the defaults from a config file. For now, just try to connect
// to a server running on the local host
func listAvailableBackups() (backups []string) {
	var err error
	if conn == nil {
		conn, err = grpc.Dial(defaultServerAddr, []grpc.DialOption{grpc.WithInsecure()}...)
		if err != nil {
			return
		}
		defer conn.Close()
	}

	mds, err := getAvailableBackups(context.TODO(), conn)
	if err != nil {
		return
	}

	for name, md := range mds {
		backup := fmt.Sprintf("%s -> %s", name, md.Description)
		backups = append(backups, backup)
	}
	return
}

func printTemplate(tpl string, data interface{}) {
	var b bytes.Buffer
	tmpl := template.Must(template.New("").Parse(tpl))
	if err := tmpl.Execute(&b, data); err != nil {
		log.Fatal(err)
	}
	print(b.String())
}

func startBackup(ctx context.Context, apiClient pbapi.ApiClient, opts *cliOptions) error {
	msg := &pbapi.RunBackupParams{
		CompressionType: pbapi.CompressionType_COMPRESSION_TYPE_NO_COMPRESSION,
		Cypher:          pbapi.Cypher_CYPHER_NO_CYPHER,
		Description:     *opts.description,
		DestinationType: pbapi.DestinationType_DESTINATION_TYPE_FILE,
	}

	switch *opts.backupType {
	case "logical":
		msg.BackupType = pbapi.BackupType_BACKUP_TYPE_LOGICAL
	case "hot":
		msg.BackupType = pbapi.BackupType_BACKUP_TYPE_HOTBACKUP
	default:
		return fmt.Errorf("backup type %q is invalid", opts.backupType)
	}

	switch *opts.destinationType {
	case "file":
		msg.DestinationType = pbapi.DestinationType_DESTINATION_TYPE_FILE
	case "aws":
		msg.DestinationType = pbapi.DestinationType_DESTINATION_TYPE_AWS
	default:
		return fmt.Errorf("destination type %v is invalid", *opts.destinationType)
	}

	switch *opts.compressionAlgorithm {
	case "":
	case "gzip":
		msg.CompressionType = pbapi.CompressionType_COMPRESSION_TYPE_GZIP
	default:
		return fmt.Errorf("compression algorithm %q ins invalid", *opts.compressionAlgorithm)
	}

	switch *opts.encryptionAlgorithm {
	case "":
	default:
		return fmt.Errorf("encryption is not implemente yet")
	}

	_, err := apiClient.RunBackup(ctx, msg)
	if err != nil {
		return err
	}

	return nil
}

func restoreBackup(ctx context.Context, apiClient pbapi.ApiClient, opts *cliOptions) error {
	msg := &pbapi.RunRestoreParams{
		MetadataFile:      *opts.restoreMetadataFile,
		SkipUsersAndRoles: *opts.restoreSkipUsersAndRoles,
	}

	_, err := apiClient.RunRestore(ctx, msg)
	if err != nil {
		return err
	}

	return nil
}

func processCliArgs(args []string) (string, *cliOptions, error) {
	app := kingpin.New("pbmctl", "Percona Backup for MongoDB CLI")
	app.Version(fmt.Sprintf("%s version %s, git commit %s", app.Name, version, commit))

	runCmd := app.Command("run", "Start a new backup or restore process")
	listCmd := app.Command("list", "List objects (connected nodes, backups, etc)")
	listBackupsCmd := listCmd.Command("backups", "List backups")
	listNodesCmd := listCmd.Command("nodes", "List objects (connected nodes, backups, etc)")
	backupCmd := runCmd.Command("backup", "Start a backup")
	restoreCmd := runCmd.Command("restore", "Restore a backup given a metadata file name")

	opts := &cliOptions{
		configFile: app.Flag("config", "Config file name").Default(defaultConfigFile).Short('c').String(),

		list:             listCmd,
		listBackups:      listBackupsCmd,
		listNodes:        listNodesCmd,
		listNodesVerbose: listNodesCmd.Flag("verbose", "Include extra node info").Bool(),

		backup:               backupCmd,
		backupType:           backupCmd.Flag("backup-type", "Backup type (logical or hot)").Default("logical").Enum("logical", "hot"),
		destinationType:      backupCmd.Flag("destination-type", "Backup destination type (file or aws)").Default("file").Enum("file", "aws"),
		compressionAlgorithm: backupCmd.Flag("compression-algorithm", "Compression algorithm used for the backup").String(),
		encryptionAlgorithm:  backupCmd.Flag("encryption-algorithm", "Encryption algorithm used for the backup").String(),
		description:          backupCmd.Flag("description", "Backup description").Required().String(),

		restore: restoreCmd,
		restoreMetadataFile: restoreCmd.Arg("metadata-file", "Metadata file having the backup info for restore").
			HintAction(listAvailableBackups).Required().String(),
		restoreSkipUsersAndRoles: restoreCmd.Flag("skip-users-and-roles", "Do not restore users and roles").Default("true").Bool(),
	}

	app.Flag("server-address", "Backup coordinator address (host:port)").Default(defaultServerAddr).Short('s').StringVar(&opts.ServerAddr)
	app.Flag("server-compressor", "Backup coordinator gRPC compression algorithm (snappy, gzip or none)").Default(snappy.Name).EnumVar(&opts.ServerCompressor, grpcCompressors...)
	app.Flag("tls", "Connection uses TLS if true, else plain TCP").Default("false").BoolVar(&opts.TLS)
	app.Flag("tls-ca-file", "The file containing the CA root cert file").ExistingFileVar(&opts.TLSCAFile)

	yamlOpts := &cliOptions{
		ServerAddr: defaultServerAddr,
	}
	if *opts.configFile != "" {
		loadOptionsFromFile(defaultConfigFile, yamlOpts)
	}

	cmd, err := app.DefaultEnvars().Parse(args)
	if err != nil {
		return "", nil, err
	}

	if cmd == "" {
		return "", opts, fmt.Errorf("Invalid command")
	}

	return cmd, opts, nil
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
	if opts.TLSCAFile == "" {
		opts.TLSCAFile = yamlOpts.TLSCAFile
	}
	if opts.ServerAddr == "" {
		opts.ServerAddr = yamlOpts.ServerAddr
	}
	if opts.ServerCompressor == "" {
		opts.ServerCompressor = yamlOpts.ServerCompressor

	}
}
