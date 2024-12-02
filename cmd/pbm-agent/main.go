package main

import (
	"context"
	"fmt"
	"github.com/alecthomas/kingpin"
	mtLog "github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/percona/percona-backup-mongodb/pbm/version"
	"github.com/spf13/cobra"
	stdlog "log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
)

const mongoConnFlag = "mongodb-uri"

func main() {
	rootCmd := &cobra.Command{
		Use:   "pbm-agent",
		Short: "Percona Backup for MongoDB",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Welcome to PBM!")
		},
	}

	helloCmd := &cobra.Command{
		Use:   "hello",
		Short: "Prints a hello message",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("Hello, world!")
		},
	}

	rootCmd.AddCommand(helloCmd)
	rootCmd.AddCommand(versionCommand())

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
	}

	var (
		pbmCmd      = kingpin.New("pbm-agent", "Percona Backup for MongoDB")
		pbmAgentCmd = pbmCmd.Command("run", "Run agent").
				Default().
				Hidden()

		mURI = pbmAgentCmd.Flag(mongoConnFlag, "MongoDB connection string").
			Envar("PBM_MONGODB_URI").
			Required().
			String()
		dumpConns = pbmAgentCmd.
				Flag("dump-parallel-collections", "Number of collections to dump in parallel").
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

		logPath = pbmCmd.Flag("log-path", "Path to file").
			Envar("LOG_PATH").
			Default("/dev/stderr").
			String()
		logJSON = pbmCmd.Flag("log-json", "Enable JSON output").
			Envar("LOG_JSON").
			Bool()
		logLevel = pbmCmd.Flag(
			"log-level",
			"Minimal log level based on severity level: D, I, W, E or F, low to high. Choosing one includes higher levels too.").
			Envar("LOG_LEVEL").
			Default(log.D).
			Enum(log.D, log.I, log.W, log.E, log.F)
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

	logOpts := &log.Opts{
		LogPath:  *logPath,
		LogLevel: *logLevel,
		LogJSON:  *logJSON,
	}

	err = runAgent(url, *dumpConns, logOpts)
	stdlog.Println("Exit:", err)
	if err != nil {
		os.Exit(1)
	}
}

func versionCommand() *cobra.Command {
	var (
		versionShort  bool
		versionCommit bool
		versionFormat string
	)

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "PBM version info",
		Run: func(cmd *cobra.Command, args []string) {
			switch {
			case versionShort:
				fmt.Println(version.Current().Short())
			case versionCommit:
				fmt.Println(version.Current().GitCommit)
			default:
				fmt.Println(version.Current().All(versionFormat))
			}
		},
	}

	versionCmd.Flags().BoolVar(&versionShort, "short", false, "Only version info")
	versionCmd.Flags().BoolVar(&versionCommit, "commit", false, "Only git commit info")
	versionCmd.Flags().StringVar(&versionFormat, "format", "", "Output format <json or \"\">")

	return versionCmd
}

func runAgent(
	mongoURI string,
	dumpConns int,
	logOpts *log.Opts,
) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	leadConn, err := connect.Connect(ctx, mongoURI, "pbm-agent")
	if err != nil {
		return errors.Wrap(err, "connect to PBM")
	}

	err = setupNewDB(ctx, leadConn)
	if err != nil {
		return errors.Wrap(err, "setup pbm collections")
	}

	agent, err := newAgent(ctx, leadConn, mongoURI, dumpConns)
	if err != nil {
		return errors.Wrap(err, "connect to the node")
	}

	logger := log.NewWithOpts(
		ctx,
		agent.leadConn,
		agent.brief.SetName,
		agent.brief.Me,
		logOpts)
	defer logger.Close()

	ctx = log.SetLoggerToContext(ctx, logger)

	mtLog.SetDateFormat(log.LogTimeFormat)
	mtLog.SetVerbosity(&options.Verbosity{VLevel: mtLog.DebugLow})
	mtLog.SetWriter(logger)

	logger.Printf(perconaSquadNotice)
	logger.Printf("log options: log-path=%s, log-level:%s, log-json:%t",
		logOpts.LogPath, logOpts.LogLevel, logOpts.LogJSON)

	canRunSlicer := true
	if err := agent.CanStart(ctx); err != nil {
		if errors.Is(err, ErrArbiterNode) || errors.Is(err, ErrDelayedNode) {
			canRunSlicer = false
		} else {
			return errors.Wrap(err, "pre-start check")
		}
	}

	agent.showIncompatibilityWarning(ctx)

	if canRunSlicer {
		go agent.PITR(ctx)
	}
	go agent.HbStatus(ctx)

	return errors.Wrap(agent.Start(ctx), "listen the commands stream")
}
