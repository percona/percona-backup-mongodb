package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/agent"
	"github.com/percona/percona-backup-mongodb/pbm"
)

func main() {
	var (
		pbmAgentCmd = kingpin.New("pbm-agent", "Percona Backup for MongoDB")
		mURI        = pbmAgentCmd.Flag("mongodb-uri", "MongoDB connection string").Envar("PBM_MONGODB_URI").Required().String()

		pprofPort = pbmAgentCmd.Flag("pport", "").Hidden().String()
	)

	_, err := pbmAgentCmd.DefaultEnvars().Parse(os.Args[1:])
	if err != nil {
		log.Println("[ERROR] Parse command line parameters:", err)
		return
	}

	if *pprofPort != "" {
		go func() {
			log.Println(http.ListenAndServe("localhost:"+*pprofPort, nil))
		}()
	}

	log.Println(runAgent(*mURI))
}

func runAgent(mongoURI string) error {
	mongoURI = "mongodb://" + strings.Replace(mongoURI, "mongodb://", "", 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	node, err := mongo.NewClient(options.Client().ApplyURI(mongoURI).SetDirect(true))
	if err != nil {
		return errors.Wrap(err, "create node client")
	}
	err = node.Connect(ctx)
	if err != nil {
		return errors.Wrap(err, "node connect")
	}

	err = node.Ping(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "node ping")
	}

	pbmClient, err := pbm.New(ctx, mongoURI)
	if err != nil {
		return errors.Wrap(err, "connect to mongodb")
	}

	agnt := agent.New(pbmClient)
	// TODO: pass only options and connect while createing a node?
	agnt.AddNode(ctx, node, mongoURI)

	fmt.Println("pbm agent is listening for the commands")
	return errors.Wrap(agnt.Start(), "listen the commands stream")
}
