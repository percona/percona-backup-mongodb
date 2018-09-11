package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kingpin"
	pbapi "github.com/percona/mongodb-backup/proto/api"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
)

type cliOptions struct {
	app        *kingpin.Application
	clientID   *string
	tls        *bool
	caFile     *string
	serverAddr *string

	listClients *kingpin.CmdClause
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

	switch cmd {
	case "list-agents":
		clients, err := getConnectedAgents(conn)
		if err != nil {
			log.Fatal(err)
		}
		printConnectedAgents(clients)
	}

}

func getConnectedAgents(conn *grpc.ClientConn) ([]*pbapi.Client, error) {
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
	return clients, nil
}

func printConnectedAgents(clients []*pbapi.Client) {
	if len(clients) == 0 {
		fmt.Println("There are no agents connected to the backup master")
		return
	}
	fmt.Println(strings.Repeat("-", 100))
	for _, client := range clients {
		fmt.Printf("%s  -  %s  -  %v\n", client.ClientID, client.Status, time.Unix(client.LastSeen, 0))
	}
}

func processCliArgs(args []string) (string, *cliOptions, error) {
	app := kingpin.New("mongodb-backup-admin", "MongoDB backup admin")
	opts := &cliOptions{
		clientID:    kingpin.Flag("client-id", "Client ID").Required().String(),
		tls:         kingpin.Flag("tls", "Connection uses TLS if true, else plain TCP").Default("false").Bool(),
		caFile:      kingpin.Flag("ca-file", "The file containning the CA root cert file").String(),
		serverAddr:  kingpin.Flag("server-addr", "The server address in the format of host:port").Default("127.0.0.1:10001").String(),
		listClients: kingpin.Command("list-agents", "List all agents connected to the server"),
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
