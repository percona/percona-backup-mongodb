package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/v2/agent"
	"github.com/percona/percona-backup-mongodb/v2/pbm"
	"github.com/percona/percona-backup-mongodb/v2/version"
)

const mongoConnFlag = "mongodb-uri"

func main() {
	var (
		pbmCmd      = kingpin.New("pbm-agent", "Percona Backup for MongoDB")
		pbmAgentCmd = pbmCmd.Command("run", "Run agent").Default().Hidden()

		mURI      = pbmAgentCmd.Flag(mongoConnFlag, "MongoDB connection string").Envar("PBM_MONGODB_URI").Required().String()
		dumpConns = pbmAgentCmd.Flag("dump-parallel-collections", "Number of collections to dump in parallel").Envar("PBM_DUMP_PARALLEL_COLLECTIONS").Default(strconv.Itoa(runtime.NumCPU() / 2)).Int()

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

	// hidecreds() will rewrite the flag content, so we have to make a copy before passing it on
	url := "mongodb://" + strings.Replace(*mURI, "mongodb://", "", 1)

	hidecreds()

	err = runAgent(url, *dumpConns)
	log.Println("Exit:", err)
	if err != nil {
		os.Exit(1)
	}
}

func runAgent(mongoURI string, dumpConns int) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pbmClient, err := pbm.New(ctx, mongoURI, "pbm-agent")
	if err != nil {
		return errors.Wrap(err, "connect to PBM")
	}

	agnt := agent.New(pbmClient)
	err = agnt.AddNode(ctx, mongoURI, dumpConns)
	if err != nil {
		return errors.Wrap(err, "connect to the node")
	}
	agnt.InitLogger(pbmClient)

	if err := agnt.CanStart(); err != nil {
		return errors.WithMessage(err, "pre-start check")
	}

	go agnt.PITR()
	go agnt.HbStatus()

	return errors.Wrap(agnt.Start(), "listen the commands stream")
}
