package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/agent"
	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/version"
)

func main() {
	var (
		pbmCmd      = kingpin.New("pbm-agent", "Percona Backup for MongoDB")
		pbmAgentCmd = pbmCmd.Command("run", "Run agent").Default().Hidden()

		mURI = pbmAgentCmd.Flag("mongodb-uri", "MongoDB connection string").Envar("PBM_MONGODB_URI").Required().String()

		versionCmd    = pbmCmd.Command("version", "PBM version info")
		versionShort  = versionCmd.Flag("short", "Only version info").Default("false").Bool()
		versionCommit = versionCmd.Flag("commit", "Only git commit info").Default("false").Bool()
		versionFormat = versionCmd.Flag("format", "Output format <json or \"\">").Default("").String()
	)

	cmd, err := pbmCmd.DefaultEnvars().Parse(os.Args[1:])
	if err != nil && cmd != versionCmd.FullCommand() {
		log.Println("Error: Parse command line parameters:", err)
		return
	}

	if cmd == versionCmd.FullCommand() {
		switch {
		case *versionCommit:
			fmt.Println(version.DefaultInfo.GitCommit)
		case *versionShort:
			fmt.Println(version.DefaultInfo.Short())
		default:
			fmt.Println(version.DefaultInfo.All(*versionFormat))
		}
		return
	}

	log.Println(runAgent(*mURI))
}

func runAgent(mongoURI string) error {
	mongoURI = "mongodb://" + strings.Replace(mongoURI, "mongodb://", "", 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pbmClient, err := pbm.New(ctx, mongoURI, "pbm-agent")
	if err != nil {
		return errors.Wrap(err, "connect to PBM")
	}

	agnt := agent.New(pbmClient)
	err = agnt.AddNode(ctx, mongoURI)
	if err != nil {
		return errors.Wrap(err, "connect to the node")
	}

	log.Println("pbm-agent", version.DefaultInfo.All(""))
	log.Println("starting pitr routine")
	go agnt.PITR()

	log.Println("listening for the commands")
	return errors.Wrap(agnt.Start(), "listen the commands stream")
}
