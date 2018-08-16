package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	pbapi "github.com/percona/mongodb-backup/proto/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
)

var (
	clientID           = flag.String("id", "", "Client ID")
	tls                = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	caFile             = flag.String("ca_file", "", "The file containning the CA root cert file")
	serverAddr         = flag.String("server_addr", "127.0.0.1:10001", "The server address in the format of host:port")
	serverHostOverride = flag.String("server_host_override", "x.test.youtube.com", "The server name use to verify the hostname returned by TLS handshake")
)

func main() {
	flag.Parse()
	var opts []grpc.DialOption
	if *tls {
		if *caFile == "" {
			*caFile = testdata.Path("ca.pem")
		}
		creds, err := credentials.NewClientTLSFromFile(*caFile, *serverHostOverride)
		if err != nil {
			log.Fatalf("Failed to create TLS credentials %v", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}

	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	processMessages(conn)

}

func processMessages(conn *grpc.ClientConn) {
	apiClient := pbapi.NewApiClient(conn)
	stream, err := apiClient.GetClients(context.Background(), &pbapi.Empty{})
	if err != nil {
		log.Fatal(err)
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%+v\n", msg)
	}
}
