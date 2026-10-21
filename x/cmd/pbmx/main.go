package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/percona/percona-backup-mongodb/x/pbm"
	"github.com/percona/percona-backup-mongodb/x/pbm/disco"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/etcd"
	"github.com/percona/percona-backup-mongodb/x/pbm/log"
)

const (
	mongoConnFlag   = "mongodb-uri"
	ctrlAgentFlag   = "ctrl-agent"
	workerAgentFlag = "worker-agent"
	nameFlag        = "name"

	etcdDataDirFlag            = "etcd-data-dir"
	etcdListenPeerPortFlag     = "etcd-listen-peer-port"
	etcdListenClientPortFlag   = "etcd-listen-client-port"
	etcdAdvertisePeerURLFlag   = "etcd-advertise-peer-url"
	etcdAdvertiseClientURLFlag = "etcd-advertise-client-url"
	etcdInitialClusterFlag     = "etcd-initial-cluster"
	etcdEndpointsFlag          = "etcd-endpoints"

	serfPortFlag = "serf-port"
	serfJoinFlag = "serf-join"

	apiSrvPortFlag = "api-srv-port"

	apiEndpointsFlag = "api-endpoints"

	logOutputsFlag = "log-outputs"
	logLevelFlag   = "log-level"
	logFormatFlag  = "log-format"

	defaultAPISrvPort    = 9595
	defaultEtcdDataDir   = "pbmx.etcd"
	defaultAPIEndpoints  = "localhost:9595"
	defaultEtcdEndpoints = "localhost:2379"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// the logger is set up in the root's PersistentPreRunE, once the flags/agent's config are parsed
	defer func() { _ = zap.L().Sync() }()

	rootCmd := rootCommand()
	rootCmd.AddCommand(backupCommand())
	rootCmd.AddCommand(listCommand())
	rootCmd.AddCommand(statusCommand())
	rootCmd.AddCommand(configCommand())
	rootCmd.AddCommand(versionCommand())
	rootCmd.AddCommand(completionCommand())

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		_ = zap.L().Sync()
		os.Exit(1)
	}
}

func rootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "pbmx",
		Short: "Percona Backup for MongoDB (experimental)",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if err := loadConfig(); err != nil {
				return err
			}
			return setupLogger()
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// worker is the default agent's role
			if viper.GetBool(ctrlAgentFlag) {
				return pbm.RunCtrlAgent(cmd.Context(), ctrlAgentConfig())
			}
			return pbm.RunWorkerAgent(cmd.Context(), workerAgentConfig())
		},
	}

	rootCmd.CompletionOptions.DisableDefaultCmd = true

	setRootFlags(rootCmd)
	return rootCmd
}

// workerAgentConfig assembles the worker-agent config from CLI/config flags.
func workerAgentConfig() *pbm.WorkerAgentConfig {
	return &pbm.WorkerAgentConfig{
		Name:          viper.GetString(nameFlag),
		MongoURI:      viper.GetString(mongoConnFlag),
		EtcdEndpoints: splitList(viper.GetString(etcdEndpointsFlag)),
		Config: disco.Config{
			SerfPort: viper.GetInt(serfPortFlag),
			SerfJoin: splitList(viper.GetString(serfJoinFlag)),
		},
	}
}

// ctrlAgentConfig assembles the ctrl-agent config from CLI/config.
// ctrl-agent is also usually worker agent so that config is reused.
func ctrlAgentConfig() *pbm.CtrlAgentConfig {
	return &pbm.CtrlAgentConfig{
		WorkerAgentConfig: *workerAgentConfig(),
		Config: etcd.Config{
			DataDir:            viper.GetString(etcdDataDirFlag),
			ListenPeerPort:     viper.GetInt(etcdListenPeerPortFlag),
			ListenClientPort:   viper.GetInt(etcdListenClientPortFlag),
			AdvertisePeerURL:   viper.GetString(etcdAdvertisePeerURLFlag),
			AdvertiseClientURL: viper.GetString(etcdAdvertiseClientURLFlag),
			InitialCluster:     viper.GetString(etcdInitialClusterFlag),
		},
		APISrvPort: viper.GetInt(apiSrvPortFlag),
	}
}

// setupLogger builds the logger from the log flags and installs it as the zap global logger.
func setupLogger() error {
	l, err := log.New(log.Config{
		Outputs: splitList(viper.GetString(logOutputsFlag)),
		Level:   viper.GetString(logLevelFlag),
		Format:  viper.GetString(logFormatFlag),
	})
	if err != nil {
		return errors.Wrap(err, "init logger")
	}

	zap.ReplaceGlobals(l)
	zap.RedirectStdLog(l)
	return nil
}

func loadConfig() error {
	cfgFile := viper.GetString("config")
	if cfgFile == "" {
		return nil
	}

	viper.SetConfigFile(cfgFile)
	if err := viper.ReadInConfig(); err != nil {
		return errors.New("failed to read config: " + err.Error())
	}
	return nil
}

func setRootFlags(rootCmd *cobra.Command) {
	rootCmd.PersistentFlags().StringP("config", "f", "", "Path to the config file")
	_ = viper.BindPFlag("config", rootCmd.PersistentFlags().Lookup("config"))

	persistentString(rootCmd, mongoConnFlag, "", "MongoDB connection string")
	persistentString(rootCmd, nameFlag, "", "Unique agent name in the cluster (also the etcd member name)")

	rootCmd.PersistentFlags().Bool(
		ctrlAgentFlag,
		false,
		"Run as a control agent (manages agent's control collections and leads the cluster)",
	)
	_ = viper.BindPFlag(ctrlAgentFlag, rootCmd.PersistentFlags().Lookup(ctrlAgentFlag))

	rootCmd.PersistentFlags().Bool(
		workerAgentFlag,
		false,
		"Run as a worker agent (performs backup/restore)",
	)
	_ = viper.BindPFlag(workerAgentFlag, rootCmd.PersistentFlags().Lookup(workerAgentFlag))

	rootCmd.MarkFlagsMutuallyExclusive(ctrlAgentFlag, workerAgentFlag)

	persistentString(rootCmd, etcdDataDirFlag, defaultEtcdDataDir,
		"Data directory for the control agent's embedded etcd")
	persistentInt(rootCmd, etcdListenPeerPortFlag, 0,
		"etcd peer listen port, bound on 0.0.0.0 (default 2380)")
	persistentInt(rootCmd, etcdListenClientPortFlag, 0,
		"etcd client listen port, bound on 0.0.0.0 (default 2379)")
	persistentString(rootCmd, etcdAdvertisePeerURLFlag, "",
		"etcd peer advertise URL (routable), e.g. http://etcd-0.example:2380")
	persistentString(rootCmd, etcdAdvertiseClientURLFlag, "",
		"etcd client advertise URL (routable), e.g. http://etcd-0.example:2379")
	persistentString(rootCmd, etcdInitialClusterFlag, "",
		"etcd initial cluster member list: name0=peerURL0,name1=peerURL1,...")
	persistentString(rootCmd, etcdEndpointsFlag, defaultEtcdEndpoints,
		"comma-separated ctrl-agent etcd client endpoints (host:port) the worker agent connects to")

	persistentInt(rootCmd, serfPortFlag, 0,
		"serf gossip listen port, bound on 0.0.0.0 (default 7946)")
	persistentString(
		rootCmd,
		serfJoinFlag,
		"",
		"comma-separated serf seed addresses (host:port) to join; "+
			"reaching any one is enough, empty starts a new cluster",
	)

	persistentInt(rootCmd, apiSrvPortFlag, defaultAPISrvPort,
		"HTTP API listen port (ctrl-agent only)")

	persistentString(
		rootCmd,
		apiEndpointsFlag,
		defaultAPIEndpoints,
		"comma-separated ctrl-agent API endpoints (host:port) for client commands; "+
			"the leader is resolved automatically",
	)

	persistentString(rootCmd, logOutputsFlag, log.StderrOutput,
		"comma-separated log targets: stdout, stderr or a file path")
	persistentString(rootCmd, logLevelFlag, log.DefaultLevel,
		"log level: debug, info, warn or error")
	persistentString(rootCmd, logFormatFlag, log.DefaultFormat,
		"log format: json or console")
}

// persistentString registers a persistent string flag, binds it to viper, and
// binds a PBM_-prefixed env var derived from the flag name as a fallback source
// (e.g. etcd-advertise-peer-url -> PBM_ETCD_ADVERTISE_PEER_URL).
func persistentString(cmd *cobra.Command, name, def, usage string) {
	cmd.PersistentFlags().String(name, def, usage)
	_ = viper.BindPFlag(name, cmd.PersistentFlags().Lookup(name))
	_ = viper.BindEnv(name, envName(name))
}

// persistentInt is the int counterpart of persistentString.
func persistentInt(cmd *cobra.Command, name string, def int, usage string) {
	cmd.PersistentFlags().Int(name, def, usage)
	_ = viper.BindPFlag(name, cmd.PersistentFlags().Lookup(name))
	_ = viper.BindEnv(name, envName(name))
}

// splitList parses a comma-separated flag value into a slice, trimming spaces
// and dropping empty entries.
func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envName(flag string) string {
	return "PBM_" + strings.ToUpper(strings.ReplaceAll(flag, "-", "_"))
}
