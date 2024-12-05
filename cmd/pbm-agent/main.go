package main

import (
	"context"
	"fmt"
	mtLog "github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/version"
	"github.com/spf13/cobra"
	stdlog "log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
)

const mongoConnFlag = "mongodb-uri"

func main() {
	var (
		mURI      string
		dumpConns int
		logPath   string
		logJSON   bool
		logLevel  string
	)

	rootCmd := &cobra.Command{
		Use:   "pbm-agent",
		Short: "Percona Backup for MongoDB",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if mURI == "" {
				return fmt.Errorf("required flag \"mongodb-uri\" not set")
			}
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			url := "mongodb://" + strings.Replace(mURI, "mongodb://", "", 1)

			hidecreds()

			logOpts := &log.Opts{
				LogPath:  logPath,
				LogLevel: logLevel,
				LogJSON:  logJSON,
			}

			err := runAgent(url, dumpConns, logOpts)
			stdlog.Println("Exit:", err)
			if err != nil {
				os.Exit(1)
			}
		},
	}

	rootCmd.AddCommand(versionCommand())

	defaultDump, err := strconv.Atoi(os.Getenv("PBM_DUMP_PARALLEL_COLLECTIONS"))
	if err != nil {
		defaultDump = runtime.NumCPU() / 2
	}

	defaultLogPath := os.Getenv("LOG_PATH")
	if defaultLogPath == "" {
		defaultLogPath = "/dev/stderr"
	}

	defaultLogJson, err := strconv.ParseBool(os.Getenv("LOG_JSON"))
	if err != nil {
		defaultLogJson = false
	}

	defaultLogLevel := os.Getenv("LOG_LEVEL")
	if defaultLogLevel == "" {
		defaultLogLevel = log.D
	}

	rootCmd.Flags().StringVar(&mURI, "mongodb-uri", os.Getenv("PBM_MONGODB_URI"), "MongoDB connection string")
	rootCmd.Flags().IntVar(&dumpConns, "dump-parallel-collections", defaultDump, "Number of collections to dump in parallel")
	rootCmd.Flags().StringVar(&logPath, "log-path", defaultLogPath, "Path to file")
	rootCmd.Flags().BoolVar(&logJSON, "log-json", defaultLogJson, "Enable JSON logging")
	rootCmd.Flags().StringVar(&logLevel, "log-level", defaultLogLevel, "Minimal log level based on severity level: D, I, W, E or F, low to high. Choosing one includes higher levels too.")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
	}

	//logLevel = pbmCmd.Flag
	//	Enum(log.D, log.I, log.W, log.E, log.F)

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
