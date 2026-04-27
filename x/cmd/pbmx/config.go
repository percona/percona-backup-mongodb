package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func configCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "config",
		Short: "Get/set PBM configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), "config executed")
			return nil
		},
	}
}
