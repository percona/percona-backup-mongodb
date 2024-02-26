package main

import (
	"context"
	"fmt"
	stdlog "log"
	"os"
	"runtime"
	"strconv"
	"strings"

	"github.com/alecthomas/kingpin"
	mtLog "github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/version"
)

const mongoConnFlag = "mongodb-uri"

func main() {
	var (
		pbmCmd      = kingpin.New("pbm-agent", "Percona Backup for MongoDB")
		pbmAgentCmd = pbmCmd.Command("run", "Run agent").
				Default().
				Hidden()

		mURI = pbmAgentCmd.Flag(mongoConnFlag, "MongoDB connection string").
			Envar("PBM_MONGODB_URI").
			Required().
			String()
		dumpConns = pbmAgentCmd.Flag("dump-parallel-collections", "Number of collections to dump in parallel").
				Envar("PBM_DUMP_PARALLEL_COLLECTIONS").
				Default(strconv.Itoa(runtime.NumCPU() / 2)).
				Int()

		versionCmd   = pbmCmd.Command("version", "PBM version info")
		versionShort = versionCmd.Flag("short", "Only version info").
				Default("false").
				Bool()
		versionCommit = versionCmd.Flag("commit", "Only git commit info").
				Default("false").
				Bool()
		versionFormat = versionCmd.Flag("format", "Output format <json or \"\">").
				Default("").
				String()
	)

	cmd, err := pbmCmd.DefaultEnvars().Parse(os.Args[1:])
	if err != nil && cmd != versionCmd.FullCommand() {
		stdlog.Println("Error: Parse command line parameters:", err)
		return
	}

	if cmd == versionCmd.FullCommand() {
		switch {
		case *versionCommit:
			fmt.Println(version.Current().GitCommit)
		case *versionShort:
			fmt.Println(version.Current().Short())
		default:
			fmt.Println(version.Current().All(*versionFormat))
		}
		return
	}

	// hidecreds() will rewrite the flag content, so we have to make a copy before passing it on
	url := "mongodb://" + strings.Replace(*mURI, "mongodb://", "", 1)

	hidecreds()

	err = runAgent(url, *dumpConns)
	stdlog.Println("Exit:", err)
	if err != nil {
		os.Exit(1)
	}
}

func runAgent(mongoURI string, dumpConns int) error {
	mtLog.SetDateFormat(log.LogTimeFormat)
	mtLog.SetVerbosity(&options.Verbosity{VLevel: mtLog.DebugLow})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leadConn, err := connect.Connect(ctx, mongoURI, "pbm-agent")
	if err != nil {
		return errors.Wrap(err, "connect to PBM")
	}

	agent, err := newAgent(ctx, leadConn, mongoURI, dumpConns)
	if err != nil {
		return errors.Wrap(err, "connect to the node")
	}

	logger := log.New(agent.leadConn.LogCollection(), agent.brief.SetName, agent.brief.Me)
	defer logger.Close()

	ctx = log.SetLoggerToContext(ctx, logger)

	if err := agent.CanStart(ctx); err != nil {
		return errors.Wrap(err, "pre-start check")
	}

	err = setupNewDB(ctx, agent.leadConn)
	if err != nil {
		return errors.Wrap(err, "setup pbm collections")
	}

	go agent.PITR(ctx)
	go agent.HbStatus(ctx)

	return errors.Wrap(agent.Start(ctx), "listen the commands stream")
}
