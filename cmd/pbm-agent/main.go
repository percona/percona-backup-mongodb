package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/alecthomas/kingpin"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/agent"
)


func main() {
	agentCmd := kingpin.New("agent", "PBM Agent")
	mURL := agentCmd.Flag("mongodb-dsn", "MongoDB connection string").String()
	nURL := agentCmd.Flag("node-dsn", "MongoDB Node connection string").String()
	// rs := agentCmd.Flag("rs", "ReplicaSet").String()

	agentCmd.DefaultEnvars().Parse(os.Args[1:])

	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://" + strings.Replace(*mURL, "mongodb://", "", 1)))
	node, err := mongo.NewClient(options.Client().ApplyURI("mongodb://" + strings.Replace(*nURL, "mongodb://", "", 1)))
	// client, err := mongo.NewClient(&options.ClientOptions{Hosts: []string{"localhost"}, })
	if err != nil {
		fmt.Fprintf(os.Stderr, "new mongo client: %v", err)
		return
	}
	// ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	ctx := context.Background()
	// defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mongo connect: %v", err)
		return
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mongo ping: %v", err)
		return
	}

	err = node.Connect(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mongo node connect: %v", err)
		return
	}

	err = node.Ping(ctx, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mongo node ping: %v", err)
		return
	}

	agnt := agent.New(client)
	agnt.AddNode(node)

	fmt.Println("listen")
	err = agnt.ListenCmd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "mongo listen cmd: %v", err)
	}
}
