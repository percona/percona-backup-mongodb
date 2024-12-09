package main

import (
	"context"
	"fmt"
	stdlog "log"
	"os"
	"os/signal"
	"runtime"
	"strings"

	mtLog "github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/version"
)

const mongoConnFlag = "mongodb-uri"

func main() {
	rootCmd := addRootCommand()
	rootCmd.AddCommand(versionCommand())

	bindFlags(rootCmd)

	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.WatchConfig()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
	}
}

func addRootCommand() *cobra.Command {
	var mURI string

	return &cobra.Command{
		Use:   "pbm-agent",
		Short: "Percona Backup for MongoDB",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			mURI = viper.GetString(mongoConnFlag)
			if mURI == "" {
				return errors.New("required flag " + mongoConnFlag + " not set")
			}

			if !isValidLogLevel(viper.GetString("log.level")) {
				return errors.New("invalid log level")
			}

			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
			url := "mongodb://" + strings.Replace(mURI, "mongodb://", "", 1)

			hidecreds()

			logOpts := &log.Opts{
				LogLevel: viper.GetString("log.level"),
				LogPath:  viper.GetString("log.path"),
				LogJSON:  viper.GetBool("log.json"),
			}

			err := runAgent(url, viper.GetInt("backup.dump-parallel-collections"), logOpts)
			stdlog.Println("Exit:", err)
			if err != nil {
				os.Exit(1)
			}
		},
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

func isValidLogLevel(logLevel string) bool {
	validLogLevels := []string{log.D, log.I, log.W, log.E, log.F}

	for _, validLevel := range validLogLevels {
		if logLevel == validLevel {
			return true
		}
	}

	return false
}

func bindFlags(cmd *cobra.Command) {
	cmd.Flags().String(mongoConnFlag, "", "MongoDB connection string")
	_ = viper.BindPFlag(mongoConnFlag, cmd.Flags().Lookup(mongoConnFlag))
	_ = viper.BindEnv(mongoConnFlag, "PBM_MONGODB_URI")

	cmd.Flags().Int("dump-parallel-collections", 0, "Number of collections to dump in parallel")
	_ = viper.BindPFlag("backup.dump-parallel-collections", cmd.Flags().Lookup("dump-parallel-collections"))
	_ = viper.BindEnv("backup.dump-parallel-collections", "PBM_DUMP_PARALLEL_COLLECTIONS")
	viper.SetDefault("backup.dump-parallel-collections", runtime.NumCPU()/2)

	cmd.Flags().String("log-path", "", "Path to file")
	_ = viper.BindPFlag("log.path", cmd.Flags().Lookup("log-path"))
	_ = viper.BindEnv("log.path", "LOG_PATH")
	viper.SetDefault("log.path", "/dev/stderr")

	cmd.Flags().Bool("log-json", false, "Enable JSON logging")
	_ = viper.BindPFlag("log.json", cmd.Flags().Lookup("log-json"))
	_ = viper.BindEnv("log.json", "LOG_PATH")
	viper.SetDefault("log.json", false)

	cmd.Flags().String("log-level", "",
		"Minimal log level based on severity level: D, I, W, E or F, low to high."+
			"Choosing one includes higher levels too.")
	_ = viper.BindPFlag("log.level", cmd.Flags().Lookup("log-level"))
	_ = viper.BindEnv("log.level", "LOG_JSON")
	viper.SetDefault("log.level", log.D)
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
