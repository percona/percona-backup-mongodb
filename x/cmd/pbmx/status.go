package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/renderer"
	"github.com/olekukonko/tablewriter/tw"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/percona/percona-backup-mongodb/x/pbm/apiclient"
	"github.com/percona/percona-backup-mongodb/x/pbm/backup"
	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage"
)

func statusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "status",
		Aliases: []string{"s"},
		Short:   "Show PBM status",
		RunE: func(cmd *cobra.Command, args []string) error {
			names, _ := cmd.Flags().GetStringArray("sections")
			debug, _ := cmd.Flags().GetBool("debug")

			sections, err := parseSections(names, debug)
			if err != nil {
				return err
			}

			endpoints := splitList(viper.GetString(apiEndpointsFlag))
			cli := apiclient.New(endpoints)

			return printStatus(cmd.Context(), cmd.OutOrStdout(), cli, sections)
		},
	}

	cmd.Flags().StringArrayP("sections", "s", nil,
		"Sections of status to display <cluster>/<pitr>/<running>/<backups>/<debug>")
	cmd.Flags().Bool("debug", false, "Show the raw state of every cluster member")

	return cmd
}

// Status sections, in display order.
const (
	sectionCluster = "cluster"
	sectionPITR    = "pitr"
	sectionRunning = "running"
	sectionBackups = "backups"
	sectionDebug   = "debug"
)

var allSections = []string{
	sectionCluster, sectionPITR, sectionRunning, sectionBackups, sectionDebug,
}

// parseSections validates the requested sections and returns them as a set.
// With no sections requested, it selects every section except debug.
func parseSections(names []string, debug bool) (map[string]bool, error) {
	sections := make(map[string]bool)
	if len(names) == 0 {
		for _, name := range allSections {
			if name != sectionDebug {
				sections[name] = true
			}
		}
	}
	if debug {
		sections[sectionDebug] = true
	}

	for _, name := range names {
		if !slices.Contains(allSections, name) {
			return nil, errors.Errorf("unknown section %q, expected one of: %s",
				name, strings.Join(allSections, ", "))
		}
		sections[name] = true
	}
	return sections, nil
}

// printStatus fetches only the data the selected sections need and renders
// them in display order, separated by blank lines.
func printStatus(ctx context.Context, w io.Writer, cli *apiclient.Client, sections map[string]bool) error {
	var members []status.AgentInfo
	if sections[sectionCluster] || sections[sectionDebug] {
		var err error
		members, err = cli.Status(ctx)
		if err != nil {
			return err
		}
	}

	first := true
	for _, name := range allSections {
		if !sections[name] {
			continue
		}
		if !first {
			fmt.Fprintln(w)
		}
		first = false

		switch name {
		case sectionCluster:
			if err := formatCluster(w, members); err != nil {
				return err
			}
		case sectionPITR:
			printHeader(w, "PITR")
			fmt.Fprintln(w, "(not implemented yet)")
		case sectionRunning:
			printHeader(w, "Currently running")
			fmt.Fprintln(w, "(not implemented yet)")
		case sectionBackups:
			cfg, err := cli.GetConfig(ctx, config.DefaultConfigName)
			if err != nil && !errors.Is(err, apiclient.ErrNotFound) {
				return err
			}
			metas, err := cli.ListBackups(ctx)
			if err != nil {
				return err
			}
			if err := formatBackupsSection(w, cfg, metas); err != nil {
				return err
			}
		case sectionDebug:
			printHeader(w, "Debug")
			if err := formatAgents(w, members); err != nil {
				return err
			}
		}
	}
	return nil
}

// printHeader prints a section title underlined with '='.
func printHeader(w io.Writer, title string) {
	title += ":"
	fmt.Fprintln(w, title)
	fmt.Fprintln(w, strings.Repeat("=", len(title)))
}

// formatCluster renders the cluster members grouped by replica set.
func formatCluster(w io.Writer, members []status.AgentInfo) error {
	printHeader(w, "Cluster")

	groups := make(map[string][]status.AgentInfo)
	for _, m := range members {
		groups[m.MongoInfo.SetName] = append(groups[m.MongoInfo.SetName], m)
	}

	setNames := make([]string, 0, len(groups))
	for name := range groups {
		setNames = append(setNames, name)
	}
	sort.Strings(setNames)

	for _, setName := range setNames {
		if setName == "" {
			// agents not attached to a mongod
			fmt.Fprintln(w, "(no mongod):")
		} else {
			fmt.Fprintf(w, "%s:\n", setName)
		}

		ms := groups[setName]
		sort.Slice(ms, func(i, j int) bool { return memberHost(ms[i]) < memberHost(ms[j]) })
		rows := make([][]string, 0, len(ms))
		for _, m := range ms {
			rows = append(rows, []string{
				"- " + memberHost(m) + memberState(m) + ":",
				agentLabel(m),
				"[v" + m.Version + "]",
				agentHealth(m),
			})
		}
		if err := renderPlainTable(w, nil, rows, []string{" ", "  ", " "}); err != nil {
			return err
		}
	}
	return nil
}

// memberHost returns the mongod host the agent is attached to, or the agent's
// own address when it has no mongod.
func memberHost(m status.AgentInfo) string {
	if m.MongoInfo.Me != "" {
		return m.MongoInfo.Me
	}
	return m.Addr + ":" + strconv.Itoa(m.Port)
}

func memberState(m status.AgentInfo) string {
	mi := m.MongoInfo
	switch {
	case mi.SetName == "":
		return ""
	case mi.IsPrimary:
		return " [P]"
	case mi.ArbiterOnly:
		return " [A]"
	case mi.Hidden:
		return " [H]"
	case mi.Secondary:
		return " [S]"
	default:
		return " [?]"
	}
}

func agentLabel(m status.AgentInfo) string {
	label := string(m.Role) + "-agent"
	if m.IsLeader {
		label += "-leader"
	}
	return label
}

func agentHealth(m status.AgentInfo) string {
	switch {
	case !m.Alive:
		return "FAILED: agent is not alive"
	case !m.AgentStatus.OK:
		return "FAILED: " + m.AgentStatus.Err
	default:
		return "OK"
	}
}

// formatBackupsSection renders the main storage and its backup snapshots.
func formatBackupsSection(w io.Writer, cfg *config.Config, metas []*backup.BackupMeta) error {
	printHeader(w, "Backups")

	fmt.Fprintln(w, "Main storage:")
	if cfg == nil {
		fmt.Fprintln(w, "  (not configured)")
	} else {
		formatStorage(w, &cfg.Storage)
	}

	fmt.Fprintln(w, "Snapshots:")
	if len(metas) == 0 {
		fmt.Fprintln(w, "  (none)")
		return nil
	}

	rows := make([][]string, 0, len(metas))
	for _, m := range metas {
		rows = append(rows, []string{
			m.Name,
			fmtSize(m.Size),
			string(m.Type),
			"", // profiles are not part of the backup metadata yet
			yesNo(isSelective(m)),
			yesNo(isBase(m)),
			fmtRestoreTime(m),
			fmtDuration(m),
			string(m.Status),
		})
	}

	header := []string{
		"Name", "Size", "Type", "Profile", "Sel", "Base", "Restore time", "Duration", "Status",
	}
	gaps := slices.Repeat([]string{"    "}, len(header)-1)
	return renderPlainTable(w, header, rows, gaps)
}

// renderPlainTable renders a borderless table indented by two spaces. gaps[i]
// separates column i from the next one. A non-empty header is underlined with
// dashes.
func renderPlainTable(w io.Writer, header []string, rows [][]string, gaps []string) error {
	lines := tw.LinesNone
	if len(header) > 0 {
		lines.ShowHeaderLine = tw.On
	}

	padding := make([]tw.Padding, 0, len(gaps)+1)
	for _, gap := range gaps {
		padding = append(padding, tw.Padding{Right: gap, Overwrite: true})
	}
	// no trailing gap after the last column
	padding = append(padding, tw.Padding{Overwrite: true})

	var buf bytes.Buffer
	table := tablewriter.NewTable(&buf,
		tablewriter.WithRenderer(renderer.NewBlueprint(tw.Rendition{
			Borders: tw.BorderNone,
			Symbols: tw.NewSymbols(tw.StyleASCII),
			Settings: tw.Settings{
				Separators: tw.Separators{
					ShowHeader:     tw.On,
					BetweenColumns: tw.Off,
					BetweenRows:    tw.Off,
				},
				Lines: lines,
			},
		})),
		tablewriter.WithHeaderAlignment(tw.AlignLeft),
		tablewriter.WithHeaderPaddingPerColumn(padding),
		tablewriter.WithRowPaddingPerColumn(padding),
	)
	if len(header) > 0 {
		table.Header(header)
	}
	if err := table.Bulk(rows); err != nil {
		return err
	}
	if err := table.Render(); err != nil {
		return err
	}

	for line := range strings.SplitSeq(strings.TrimRight(buf.String(), "\n"), "\n") {
		fmt.Fprintf(w, "  %s\n", strings.TrimRight(line, " "))
	}
	return nil
}

func fmtRestoreTime(m *backup.BackupMeta) string {
	if m.Status != backup.StatusDone {
		return ""
	}
	return fmtBSONTS(m.LastWriteTS)
}

// formatStorage renders the storage type and its location.
func formatStorage(w io.Writer, stg *config.StorageConf) {
	switch stg.Type {
	case storage.Filesystem:
		fmt.Fprintf(w, "  %-12s%s\n", "Type:", "FS")
		if stg.Filesystem != nil {
			fmt.Fprintf(w, "  %-12s%s\n", "Path:", stg.Filesystem.Path)
		}
	default:
		fmt.Fprintf(w, "  %-12s%s\n", "Type:", strings.ToUpper(string(stg.Type)))
	}
}

// formatAgents renders the cluster members as a table.
func formatAgents(w io.Writer, members []status.AgentInfo) error {
	table := tablewriter.NewWriter(w)
	table.Header([]string{
		"Name", "Addr", "Port", "APIPort", "Role", "Version", "IsLeader", "Alive",
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
			m.Version,
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
