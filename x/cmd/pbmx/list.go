package main

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-backup-mongodb/x/pbm/apiclient"
	"github.com/percona/percona-backup-mongodb/x/pbm/backup"
	"github.com/percona/percona-backup-mongodb/x/pbm/defs"
)

// tsLayout is the timestamp layout used across the backup listing (UTC).
const tsLayout = "2006-01-02T15:04:05"

func listCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List backups",
		RunE: func(cmd *cobra.Command, args []string) error {
			full, _ := cmd.Flags().GetBool("full")

			endpoints := splitList(viper.GetString(apiEndpointsFlag))
			cli := apiclient.New(endpoints)

			metas, err := cli.ListBackups(cmd.Context())
			if err != nil {
				return err
			}

			out := cmd.OutOrStdout()
			if full {
				return formatBackupsFull(out, metas)
			}
			return formatBackups(out, metas)
		},
	}

	cmd.Flags().Bool("full", false, "Show extended backup info")

	return cmd
}

// formatBackups renders the compact backup snapshot listing.
func formatBackups(w io.Writer, metas []*backup.BackupMeta) error {
	fmt.Fprintln(w, "Backup snapshots:")

	table := tablewriter.NewWriter(w)
	table.Header([]string{
		"Name", "Type", "Profile", "Selective", "Base", "Restore time",
	})

	for _, m := range metas {
		if err := table.Append([]string{
			m.Name,
			string(m.Type),
			"", // profiles are not part of the backup metadata yet
			yesNo(isSelective(m)),
			yesNo(isBase(m)),
			fmtBSONTS(m.LastWriteTS),
		}); err != nil {
			return err
		}
	}

	return table.Render()
}

// formatBackupsFull renders every backup metadata field worth showing.
func formatBackupsFull(w io.Writer, metas []*backup.BackupMeta) error {
	fmt.Fprintln(w, "Backup snapshots:")

	table := tablewriter.NewWriter(w)
	table.Header([]string{
		"Name", "Type", "Status", "Size", "Compression",
		"Selective", "Base", "Source backup", "Replsets", "Mongo version", "FCV",
		"PBM version", "Start time", "Restore time", "Error",
	})

	for _, m := range metas {
		if err := table.Append([]string{
			m.Name,
			string(m.Type),
			string(m.Status),
			strconv.FormatInt(m.Size, 10),
			string(m.Compression),
			yesNo(isSelective(m)),
			yesNo(isBase(m)),
			m.SrcBackup,
			replsetNames(m),
			m.MongoVersion,
			m.FCV,
			m.PBMVersion,
			fmtUnixSec(m.StartTS),
			fmtBSONTS(m.LastWriteTS),
			m.Err,
		}); err != nil {
			return err
		}
	}

	return table.Render()
}

// isSelective reports whether the backup targets a subset of the data.
func isSelective(m *backup.BackupMeta) bool {
	return len(m.Namespaces) > 0 || m.SelUsersAndRoles
}

// isBase reports whether an incremental backup is a base (has no source backup).
func isBase(m *backup.BackupMeta) bool {
	return m.Type == defs.IncrementalBackup && m.SrcBackup == ""
}

// replsetNames joins the backup's replset names.
func replsetNames(m *backup.BackupMeta) string {
	names := make([]string, 0, len(m.Replsets))
	for _, rs := range m.Replsets {
		names = append(names, rs.Name)
	}
	return strings.Join(names, ",")
}

func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// fmtBSONTS formats a BSON timestamp as UTC, or "" when unset.
func fmtBSONTS(ts bson.Timestamp) string {
	if ts.T == 0 {
		return ""
	}
	return time.Unix(int64(ts.T), 0).UTC().Format(tsLayout)
}

// fmtUnixSec formats Unix seconds as UTC, or "" when unset.
func fmtUnixSec(sec int64) string {
	if sec <= 0 {
		return ""
	}
	return time.Unix(sec, 0).UTC().Format(tsLayout)
}
