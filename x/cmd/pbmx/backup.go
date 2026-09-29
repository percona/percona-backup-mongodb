package main

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/percona/percona-backup-mongodb/x/pbm/apiclient"
	"github.com/percona/percona-backup-mongodb/x/pbm/backup"
	"github.com/percona/percona-backup-mongodb/x/pbm/compress"
	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/defs"
)

// backup command flags.
const (
	typeFlag             = "type"
	compressionFlag      = "compression"
	compressionLevelFlag = "compression-level"
	numParallelFilesFlag = "num-parallel-files"
	profileFlag          = "profile"
)

func backupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Make a backup",
		RunE: func(cmd *cobra.Command, args []string) error {
			endpoints := splitList(viper.GetString(apiEndpointsFlag))
			cli := apiclient.New(endpoints)

			t, err := cli.StartBackup(cmd.Context(), backupOptions(cmd))
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

	cmd.Flags().StringP(typeFlag, "t", string(defs.LogicalBackup),
		"backup type: <physical>/<logical>/<incremental>/<external>")
	cmd.Flags().String(compressionFlag, "",
		"Compression type <none>/<gzip>/<snappy>/<lz4>/<s2>/<pgzip>/<zstd>")
	cmd.Flags().IntSlice(compressionLevelFlag, nil,
		"Compression level (specific to the compression type)")
	cmd.Flags().Int32(numParallelFilesFlag, 0,
		"Number of files to upload in parallel (physical backup, filesystem storage only)")
	cmd.Flags().String(profileFlag, config.DefaultConfigName, "Config profile name")

	return cmd
}

// backupOptions assembles the backup options from the command's flags. It
// doesn't check them: the API is the one to accept or refuse a backup.
func backupOptions(cmd *cobra.Command) backup.Options {
	f := cmd.Flags()
	typ, _ := f.GetString(typeFlag)
	compression, _ := f.GetString(compressionFlag)
	level, _ := f.GetIntSlice(compressionLevelFlag)
	numParallelFiles, _ := f.GetInt32(numParallelFilesFlag)
	profile, _ := f.GetString(profileFlag)

	opts := backup.Options{
		Type:             defs.BackupType(typ),
		Compression:      compress.CompressionType(compression),
		NumParallelFiles: numParallelFiles,
		Profile:          profile,
	}
	// the level is a list to keep the flag's legacy shape; a compression
	// takes a single level, so only the first value is passed on.
	if len(level) > 0 {
		opts.CompressionLevel = &level[0]
	}

	return opts
}
