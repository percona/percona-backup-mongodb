package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/percona/percona-backup-mongodb/x/pbm"
	"github.com/percona/percona-backup-mongodb/x/pbm/disco"
	"github.com/percona/percona-backup-mongodb/x/pbm/etcd"
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

	serfPortFlag = "serf-port"
	serfJoinFlag = "serf-join"

	apiSrvPortFlag = "api-srv-port"

	defaultEtcdDataDir = "pbmx.etcd"
	defaultAPISrvPort  = 9595
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rootCmd := rootCommand()
	rootCmd.AddCommand(backupCommand())
	rootCmd.AddCommand(statusCommand())
	rootCmd.AddCommand(configCommand())
	rootCmd.AddCommand(versionCommand())
	rootCmd.AddCommand(completionCommand())

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func rootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "pbmx",
		Short: "Percona Backup for MongoDB (experimental)",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return loadConfig()
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
		Name:     viper.GetString(nameFlag),
		MongoURI: viper.GetString(mongoConnFlag),
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
		ctrlAgentFlag, false, "Run as a control agent (manages agent's control collections and leads the cluster)")
	_ = viper.BindPFlag(ctrlAgentFlag, rootCmd.PersistentFlags().Lookup(ctrlAgentFlag))

	rootCmd.PersistentFlags().Bool(
		workerAgentFlag, false, "Run as a worker agent (performs backup/restore)")
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

	persistentInt(rootCmd, serfPortFlag, 0,
		"serf gossip listen port, bound on 0.0.0.0 (default 7946)")
	persistentString(rootCmd, serfJoinFlag, "",
		"comma-separated serf seed addresses (host:port) to join; reaching any one is enough, empty starts a new cluster")

	persistentInt(rootCmd, apiSrvPortFlag, defaultAPISrvPort,
		"HTTP API listen port (ctrl-agent only)")
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
