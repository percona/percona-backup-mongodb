package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/percona/percona-backup-mongodb/x/pbm"
)

const (
	mongoConnFlag   = "mongodb-uri"
	ctrlAgentFlag   = "ctrl-agent"
	workerAgentFlag = "worker-agent"

	etcdDataDirFlag = "etcd-data-dir"

	defaultEtcdDataDir = "pbmx.etcd"
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
			if viper.GetBool(ctrlAgentFlag) {
				return pbm.RunCtrlAgent(cmd.Context(), viper.GetString(etcdDataDirFlag))
			}
			// worker agent has no etcd; its run path lands here later.
			return cmd.Help()
		},
	}

	rootCmd.CompletionOptions.DisableDefaultCmd = true

	setRootFlags(rootCmd)
	return rootCmd
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

	rootCmd.PersistentFlags().String(mongoConnFlag, "", "MongoDB connection string")
	_ = viper.BindPFlag(mongoConnFlag, rootCmd.PersistentFlags().Lookup(mongoConnFlag))
	_ = viper.BindEnv(mongoConnFlag, "PBM_MONGODB_URI")

	rootCmd.PersistentFlags().Bool(
		ctrlAgentFlag, false, "Run as a control agent (manages agent's control collections and leads the cluster)")
	_ = viper.BindPFlag(ctrlAgentFlag, rootCmd.PersistentFlags().Lookup(ctrlAgentFlag))

	rootCmd.PersistentFlags().Bool(
		workerAgentFlag, false, "Run as a worker agent (performs backup/restore)")
	_ = viper.BindPFlag(workerAgentFlag, rootCmd.PersistentFlags().Lookup(workerAgentFlag))

	rootCmd.MarkFlagsMutuallyExclusive(ctrlAgentFlag, workerAgentFlag)

	rootCmd.PersistentFlags().String(
		etcdDataDirFlag, defaultEtcdDataDir, "Data directory for the control agent's embedded etcd")
	_ = viper.BindPFlag(etcdDataDirFlag, rootCmd.PersistentFlags().Lookup(etcdDataDirFlag))
}
