package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func statusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show PBM status",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Fprintln(cmd.OutOrStdout(), "status executed")
			return nil
		},
	}
}
