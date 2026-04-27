package main

import (
	"github.com/spf13/cobra"
)

func versionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "pbmx version info",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Println("pbmx version (dev)")
		},
	}
}
