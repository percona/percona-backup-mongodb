package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	mongoConnFlag   = "mongodb-uri"
	ctrlAgentFlag   = "ctrl-agent"
	workerAgentFlag = "worker-agent"
)

func main() {
	rootCmd := rootCommand()
	rootCmd.AddCommand(backupCommand())
	rootCmd.AddCommand(statusCommand())
	rootCmd.AddCommand(configCommand())
	rootCmd.AddCommand(versionCommand())
	rootCmd.AddCommand(completionCommand())

	if err := rootCmd.Execute(); err != nil {
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

	rootCmd.PersistentFlags().Bool(ctrlAgentFlag, false, "Run as a control agent (additionally manages control collections)")
	_ = viper.BindPFlag(ctrlAgentFlag, rootCmd.PersistentFlags().Lookup(ctrlAgentFlag))

	rootCmd.PersistentFlags().Bool(workerAgentFlag, false, "Run as a worker agent (performs backup/restore)")
	_ = viper.BindPFlag(workerAgentFlag, rootCmd.PersistentFlags().Lookup(workerAgentFlag))

	rootCmd.MarkFlagsMutuallyExclusive(ctrlAgentFlag, workerAgentFlag)
}
