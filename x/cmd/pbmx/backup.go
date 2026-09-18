package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/percona/percona-backup-mongodb/x/pbm/apiclient"
)

func backupCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "backup",
		Short: "Make a backup",
		RunE: func(cmd *cobra.Command, args []string) error {
			endpoints := splitList(viper.GetString(apiEndpointsFlag))
			cli := apiclient.New(endpoints)

			t, err := cli.StartBackup(cmd.Context())
			if err != nil {
				return err
			}

			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "Starting backup '%s'\n", t.Name)
			fmt.Fprintf(out, "Agents: %s\n", strings.Join(t.Agents, ", "))
			fmt.Fprintf(out, "Leader: %s\n", t.Leader)
			return nil
		},
	}
}
