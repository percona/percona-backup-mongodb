package main

import (
	"context"
	"fmt"
	stdlog "log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"

	"github.com/alecthomas/kingpin"
	mtLog "github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/version"
)

const mongoConnFlag = "mongodb-uri"

func main() {
	var (
		pbmCmd = kingpin.New("pbm-agent", "Percona Backup for MongoDB")

		pbmAgentCmd = pbmCmd.Command("run", "Run agent").
				Default().
				Hidden()

		logPathFlag = pbmCmd.Flag("log-path", "path to file").
				Default("/dev/stderr").
				String()
		logJSONFlag  = pbmCmd.Flag("log-json", "enable JSON output").Bool()
		logLevelFlag = pbmCmd.Flag("log-level",
			fmt.Sprintf("minimal log level (options: %q, %q, %q, %q)",
				log.DebugLevel.LongString(),
				log.InfoLevel.LongString(),
				log.WarningLevel.LongString(),
				log.ErrorLevel.LongString())).
			Default(log.DebugLevel.LongString()).
			Enum(
				log.DebugLevel.LongString(),
				log.InfoLevel.LongString(),
				log.WarningLevel.LongString(),
				log.ErrorLevel.LongString())

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

	fmt.Print(perconaSquadNotice)

	err = runAgent(url, *dumpConns, *logPathFlag, log.ParseLevel(*logLevelFlag), *logJSONFlag)
	stdlog.Println("Exit:", err)
	if err != nil {
		os.Exit(1)
	}
}

func runAgent(
	mongoURI string,
	dumpConns int,
	logPath string,
	logLevel log.Level,
	logJSON bool,
) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer cancel()

	initCtx := log.Context(ctx, log.WithEvent("init"))

	leadConn, err := connect.Connect(initCtx, mongoURI, "pbm-agent")
	if err != nil {
		return errors.Wrap(err, "connect to PBM")
	}

	agent, err := newAgent(initCtx, leadConn, mongoURI, dumpConns)
	if err != nil {
		return errors.Wrap(err, "connect to the node")
	}

	err = setupLogging(initCtx, leadConn, logPath, logLevel, logJSON)
	if err != nil {
		return errors.Wrap(err, "setup logging")
	}
	defer func() {
		// flush logs
		log.SetRemoteHandler(nil).Close()
		log.SetLocalHandler(nil).Close()
	}()

	canRunSlicer := true
	if err := agent.CanStart(initCtx); err != nil {
		if errors.Is(err, ErrArbiterNode) || errors.Is(err, ErrDelayedNode) {
			canRunSlicer = false
		} else {
			return errors.Wrap(err, "pre-start check")
		}
	}

	err = setupNewDB(initCtx, agent.leadConn)
	if err != nil {
		return errors.Wrap(err, "setup pbm collections")
	}

	agent.showIncompatibilityWarning(initCtx)

	if canRunSlicer {
		go agent.PITR(ctx)
	}
	go agent.HbStatus(ctx)

	return errors.Wrap(agent.Start(ctx), "listen the commands stream")
}

func setupLogging(
	ctx context.Context,
	conn connect.Client,
	logpath string,
	level log.Level,
	json bool,
) error {
	mtLog.SetDateFormat(log.LogTimeFormat)
	mtLog.SetVerbosity(&options.Verbosity{VLevel: mtLog.DebugLow})
	mtLog.SetWriter(logWriter{}) // include mongo-tools logs

	// ensure log collection before init
	err := ensureLogCollection(ctx, conn)
	if err != nil {
		return errors.Wrap(err, "create log collection")
	}

	log.SetRemoteHandler(log.NewMongoHandler(conn.MongoClient()))

	cfg, err := config.GetConfig(ctx, conn)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return errors.Wrap(err, "setup logging: get config")
		}
		cfg = &config.Config{}
	}

	if cfg.Logging != nil {
		if cfg.Logging.Path != "" {
			logpath = cfg.Logging.Path
		}
		if cfg.Logging.Level != "" {
			level = cfg.Logging.Level.Level()
		}
		if json != cfg.Logging.JSON {
			json = cfg.Logging.JSON
		}
	}

	h, err := log.NewFileHandler(logpath, level, json)
	if err != nil {
		return errors.Wrap(err, "setup logging: new file log handler")
	}

	log.SetLocalHandler(h)

	return nil
}

type logWriter struct{}

func (w logWriter) Write(m []byte) (int, error) {
	log.DebugAttrs(nil, string(m))
	return len(m), nil
}
