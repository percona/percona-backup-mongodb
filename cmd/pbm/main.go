package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kingpin"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/agent"
	"github.com/percona/percona-backup-mongodb/pbm"
)

var (
	pbmCmd = kingpin.New("pbm", "Percona Backup for MongoDB")
	mURL   = pbmCmd.Flag("mongodb-dsn", "MongoDB connection string").Required().String()

	agentCmd = pbmCmd.Command("agent", "Run the agent mode")
	nURL     = agentCmd.Flag("node-dsn", "MongoDB Node connection string").String()

	storageCmd     = pbmCmd.Command("storage", "Target storage")
	storageSetCmd  = storageCmd.Command("set", "Set storage")
	storageConfig  = storageSetCmd.Flag("config", "Storage config file in yaml format").String()
	storageShowCmd = storageCmd.Command("show", "Show current storage configuration")

	client *mongo.Client
)

func main() {
	cmd, err := pbmCmd.DefaultEnvars().Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "[ERROR] Parse command line parameters:", err)
	}

	client, err = mongo.NewClient(options.Client().ApplyURI("mongodb://" + strings.Replace(*mURL, "mongodb://", "", 1)))
	if err != nil {
		fmt.Fprintf(os.Stderr, "new mongo client: %v", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
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

	switch cmd {
	case agentCmd.FullCommand():
		runAgent()
	case storageSetCmd.FullCommand():
		buf, err := ioutil.ReadFile(*storageConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Unable to read storage file: %v", err)
			return
		}
		err = pbm.New(client).SetStorageByte(buf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Unable to set storage: %v", err)
			return
		}
		fmt.Println("[Done]")
	case storageShowCmd.FullCommand():
		stg, err := pbm.New(client).GetStorageYaml(true)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Unable to get storage: %v", err)
			return
		}
		fmt.Printf("Storage\n-------\n%s\n", stg)
	}
}

func runAgent() {
	nodeURI := "mongodb://" + strings.Replace(*nURL, "mongodb://", "", 1)
	node, err := mongo.NewClient(options.Client().ApplyURI(nodeURI))
	if err != nil {
		fmt.Fprintf(os.Stderr, "new mongo client for node: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
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
	// TODO: pass only options and connect while createing a node?
	agnt.AddNode(node, nodeURI)

	fmt.Println("listen")
	err = agnt.ListenCmd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "mongo listen cmd: %v", err)
	}
}
