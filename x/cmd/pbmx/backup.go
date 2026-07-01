package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func backupCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "backup",
		Short: "Make a backup",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), "backup executed")
			return nil
		},
	}
}
