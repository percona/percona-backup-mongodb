package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/alecthomas/kingpin"
	pbapi "github.com/percona/mongodb-backup/proto/api"
	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
)

type cliOptions struct {
	app        *kingpin.Application
	tls        *bool
	caFile     *string
	serverAddr *string

	listClients *kingpin.CmdClause

	startBackup          *kingpin.CmdClause
	backupType           *string
	destinationType      *string
	compressionAlgorithm *string
	encryptionAlgorithm  *string
	description          *string

	restore         *kingpin.CmdClause
	restoreMetadata *string

	listBackups *kingpin.CmdClause
}

func main() {
	cmd, opts, err := processCliArgs(os.Args[1:])
	if err != nil {
		if opts != nil {
			kingpin.Usage()
		}
		log.Fatal(err)
	}

	var grpcOpts []grpc.DialOption
	if *opts.tls {
		if *opts.caFile == "" {
			*opts.caFile = testdata.Path("ca.pem")
		}
		creds, err := credentials.NewClientTLSFromFile(*opts.caFile, "")
		if err != nil {
			log.Fatalf("Failed to create TLS credentials %v", err)
		}
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(creds))
	} else {
		grpcOpts = append(grpcOpts, grpc.WithInsecure())
	}

	conn, err := grpc.Dial(*opts.serverAddr, grpcOpts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	apiClient := pbapi.NewApiClient(conn)
	if err != nil {
		log.Fatalf("Cannot connect to the API: %s", err)
	}

	switch cmd {
	case "list-agents":
		clients, err := connectedAgents(conn)
		if err != nil {
			log.Errorf("Cannot get the list of connected agents: %s", err)
			break
		}
		printConnectedAgents(clients)
	case "list-backups":
		md, err := getAvailableBackups(conn)
		if err != nil {
			log.Errorf("Cannot get the list of available backups: %s", err)
			break
		}
		if len(md) > 0 {
			printAvailableBackups(md)
			return
		}
		fmt.Println("No backups found")
	case "start-backup":
		err := startBackup(apiClient, opts)
		if err != nil {
			log.Fatal(err)
			log.Fatalf("Cannot send the StartBackup command to the gRPC server: %s", err)
		}
	}

}

func connectedAgents(conn *grpc.ClientConn) ([]*pbapi.Client, error) {
	apiClient := pbapi.NewApiClient(conn)
	stream, err := apiClient.GetClients(context.Background(), &pbapi.Empty{})
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

func getAvailableBackups(conn *grpc.ClientConn) (map[string]*pb.BackupMetadata, error) {
	apiClient := pbapi.NewApiClient(conn)
	stream, err := apiClient.BackupsMetadata(context.Background(), &pbapi.Empty{})
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

func printAvailableBackups(md map[string]*pb.BackupMetadata) {
	for name, _ := range md {
		fmt.Println(name)
	}
}

func printConnectedAgents(clients []*pbapi.Client) {
	if len(clients) == 0 {
		fmt.Println("There are no agents connected to the backup master")
		return
	}
	fmt.Println("      Node Name       -    Node Type         -     Replicaset     -  Backup Running  -            Last Seen")
	for _, client := range clients {
		runningBackup := "false"
		if client.Status.GetRunningDBBackup() {
			runningBackup = "true"
		}
		fmt.Printf("%20s  - %20s - %18s -       %5s      -  %v\n", client.ID, client.NodeType, client.ReplicasetName, runningBackup, time.Unix(client.LastSeen, 0))
	}
}

func startBackup(apiClient pbapi.ApiClient, opts *cliOptions) error {
	msg := &pbapi.RunBackupParams{
		CompressionType: pbapi.CompressionType_NO_COMPRESSION,
		Cypher:          pbapi.Cypher_NO_CYPHER,
		Description:     *opts.description,
	}

	switch *opts.backupType {
	case "logical":
		msg.BackupType = pbapi.BackupType_LOGICAL
	case "hot":
		msg.BackupType = pbapi.BackupType_LOGICAL
	}

	switch *opts.destinationType {
	case "logical":
		msg.DestinationType = pbapi.DestinationType_FILE
	case "aws":
		msg.DestinationType = pbapi.DestinationType_AWS
	}

	switch *opts.compressionAlgorithm {
	case "gzip":
		msg.CompressionType = pbapi.CompressionType_GZIP
	}

	switch *opts.encryptionAlgorithm {
	}

	_, err := apiClient.RunBackup(context.Background(), msg)
	if err != nil {
		return err
	}

	return nil
}

func processCliArgs(args []string) (string, *cliOptions, error) {
	app := kingpin.New("mongodb-backup-admin", "MongoDB backup admin")
	listClientsCmd := app.Command("list-agents", "List all agents connected to the server")
	listBackups := app.Command("list-backups", "List all backups (metadata files) stored in the server working directory")
	startBackupCmd := app.Command("start-backup", "Start a backup")

	opts := &cliOptions{
		tls:        app.Flag("tls", "Connection uses TLS if true, else plain TCP").Default("false").Bool(),
		caFile:     app.Flag("ca-file", "The file containning the CA root cert file").String(),
		serverAddr: app.Flag("server-addr", "The server address in the format of host:port").Default("127.0.0.1:10001").String(),

		listClients: listClientsCmd,

		startBackup:          startBackupCmd,
		backupType:           startBackupCmd.Flag("backup-type", "Backup type").Enum("logical", "hot"),
		destinationType:      startBackupCmd.Flag("destination-type", "Backup destination type").Enum("file", "aws"),
		compressionAlgorithm: startBackupCmd.Flag("compression-algorithm", "Compression algorithm used for the backup").String(),
		encryptionAlgorithm:  startBackupCmd.Flag("encryption-algorithm", "Encryption algorithm used for the backup").String(),
		description:          startBackupCmd.Flag("description", "Backup description").Required().String(),
		listBackups:          listBackups,
	}

	cmd, err := app.Parse(args)
	if err != nil {
		return "", nil, err
	}

	if cmd == "" {
		return "", opts, fmt.Errorf("Invalid command")
	}

	return cmd, opts, nil
}
