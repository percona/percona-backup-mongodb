package main

import (
	"io"
	"strconv"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/percona/percona-backup-mongodb/x/pbm/apiclient"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

func statusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show PBM status",
		RunE: func(cmd *cobra.Command, args []string) error {
			endpoints := splitList(viper.GetString(apiEndpointsFlag))
			cli := apiclient.New(endpoints)

			members, err := cli.Status(cmd.Context())
			if err != nil {
				return err
			}

			return formatAgents(cmd.OutOrStdout(), members)
		},
	}
}

// formatAgents renders the cluster members as a table.
func formatAgents(w io.Writer, members []status.AgentInfo) error {
	table := tablewriter.NewWriter(w)
	table.Header([]string{
		"Name", "Addr", "Port", "APIPort", "Role", "IsLeader", "Alive",
		"SetName", "Me", "Sharded", "ConfigSvr", "IsPrimary", "Secondary",
		"Hidden", "Passive", "ArbiterOnly", "MongoStatus", "AgentOK", "AgentErr",
	})

	for _, m := range members {
		mi := m.MongoInfo
		if err := table.Append([]string{
			m.Name,
			m.Addr,
			strconv.Itoa(m.Port),
			strconv.Itoa(m.APIPort),
			string(m.Role),
			strconv.FormatBool(m.IsLeader),
			strconv.FormatBool(m.Alive),
			mi.SetName,
			mi.Me,
			strconv.FormatBool(mi.Sharded),
			strconv.FormatBool(mi.ConfigSvr),
			strconv.FormatBool(mi.IsPrimary),
			strconv.FormatBool(mi.Secondary),
			strconv.FormatBool(mi.Hidden),
			strconv.FormatBool(mi.Passive),
			strconv.FormatBool(mi.ArbiterOnly),
			mi.Status,
			strconv.FormatBool(m.AgentStatus.OK),
			m.AgentStatus.Err,
		}); err != nil {
			return err
		}
	}

	return table.Render()
}
