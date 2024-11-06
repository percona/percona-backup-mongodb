package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kingpin"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
	"github.com/percona/percona-backup-mongodb/pbm/version"
	"github.com/percona/percona-backup-mongodb/sdk"
)

const (
	datetimeFormat = "2006-01-02T15:04:05"
	dateFormat     = "2006-01-02"
)

const (
	RSMappingEnvVar = "PBM_REPLSET_REMAPPING"
	RSMappingFlag   = "replset-remapping"
	RSMappingDoc    = "re-map replset names for backups/oplog (e.g. to_name_1=from_name_1,to_name_2=from_name_2)"
)

type outFormat string

const (
	outJSON       outFormat = "json"
	outJSONpretty outFormat = "json-pretty"
	outText       outFormat = "text"
)

type logsOpts struct {
	tail     int64
	node     string
	severity string
	event    string
	opid     string
	location string
	extr     bool
	follow   bool
}

type cliResult interface {
	HasError() bool
}

func main() {
	var (
		pbmCmd = kingpin.New("pbm", "Percona Backup for MongoDB")
		mURL   = pbmCmd.Flag("mongodb-uri",
			"MongoDB connection string (Default = PBM_MONGODB_URI environment variable)").
			Envar("PBM_MONGODB_URI").
			String()
		pbmOutFormat = pbmCmd.Flag("out", "Output format <text>/<json>").
				Short('o').
				Default(string(outText)).
				Enum(string(outJSON), string(outJSONpretty), string(outText))
	)
	pbmCmd.HelpFlag.Short('h')

	versionCmd := pbmCmd.Command("version", "PBM version info")
	versionShort := versionCmd.Flag("short", "Show only version info").
		Short('s').
		Default("false").
		Bool()
	versionCommit := versionCmd.Flag("commit", "Show only git commit info").
		Short('c').
		Default("false").
		Bool()

	configCmd := pbmCmd.Command("config", "Set, change or list the config")
	cfg := configOpts{set: make(map[string]string)}
	configCmd.Flag("force-resync", "Resync backup list with the current store").
		BoolVar(&cfg.rsync)
	configCmd.Flag("list", "List current settings").
		BoolVar(&cfg.list)
	configCmd.Flag("file", "Upload config from YAML file").
		StringVar(&cfg.file)
	configCmd.Flag("set", "Set the option value <key.name=value>").
		StringMapVar(&cfg.set)
	configCmd.Arg("key", "Show the value of a specified key").
		StringVar(&cfg.key)
	configCmd.Flag("wait", "Wait for finish").
		Short('w').
		BoolVar(&cfg.wait)
	configCmd.Flag("wait-time", "Maximum wait time").
		DurationVar(&cfg.waitTime)

	configProfileCmd := pbmCmd.
		Command("profile", "Configuration profiles")

	listConfigProfileCmd := configProfileCmd.
		Command("list", "List configuration profiles").
		Default()

	showConfigProfileOpts := showConfigProfileOptions{}
	showConfigProfileCmd := configProfileCmd.
		Command("show", "Show configuration profile")
	showConfigProfileCmd.
		Arg("profile-name", "Profile name").
		Required().
		StringVar(&showConfigProfileOpts.name)

	addConfigProfileOpts := addConfigProfileOptions{}
	addConfigProfileCmd := configProfileCmd.
		Command("add", "Save configuration profile")
	addConfigProfileCmd.
		Arg("profile-name", "Profile name").
		Required().
		StringVar(&addConfigProfileOpts.name)
	addConfigProfileCmd.
		Arg("file", "Path to configuration file").
		Required().
		FileVar(&addConfigProfileOpts.file)
	addConfigProfileCmd.
		Flag("sync", "Sync from the external storage").
		BoolVar(&addConfigProfileOpts.sync)
	addConfigProfileCmd.
		Flag("wait", "Wait for done by agents").
		Short('w').
		BoolVar(&addConfigProfileOpts.wait)
	addConfigProfileCmd.Flag("wait-time", "Maximum wait time").
		DurationVar(&addConfigProfileOpts.waitTime)

	removeConfigProfileOpts := removeConfigProfileOptions{}
	removeConfigProfileCmd := configProfileCmd.
		Command("remove", "Remove configuration profile")
	removeConfigProfileCmd.
		Arg("profile-name", "Profile name").
		Required().
		StringVar(&removeConfigProfileOpts.name)
	removeConfigProfileCmd.
		Flag("wait", "Wait for done by agents").
		Short('w').
		BoolVar(&removeConfigProfileOpts.wait)
	removeConfigProfileCmd.Flag("wait-time", "Maximum wait time").
		DurationVar(&removeConfigProfileOpts.waitTime)

	syncConfigProfileOpts := syncConfigProfileOptions{}
	syncConfigProfileCmd := configProfileCmd.
		Command("sync", "Sync backup list from configuration profile")
	syncConfigProfileCmd.
		Arg("profile-name", "Profile name").
		StringVar(&syncConfigProfileOpts.name)
	syncConfigProfileCmd.
		Flag("all", "Sync from all external storages").
		BoolVar(&syncConfigProfileOpts.all)
	syncConfigProfileCmd.
		Flag("clear", "Clear backup list (can be used with profile name or --all)").
		BoolVar(&syncConfigProfileOpts.clear)
	syncConfigProfileCmd.
		Flag("wait", "Wait for done by agents").
		Short('w').
		BoolVar(&syncConfigProfileOpts.wait)
	syncConfigProfileCmd.Flag("wait-time", "Maximum wait time").
		DurationVar(&syncConfigProfileOpts.waitTime)

	backupCmd := pbmCmd.Command("backup", "Make backup")
	backupOptions := backupOpts{}
	backupCmd.Flag("compression", "Compression type <none>/<gzip>/<snappy>/<lz4>/<s2>/<pgzip>/<zstd>").
		EnumVar(&backupOptions.compression,
			string(compress.CompressionTypeNone),
			string(compress.CompressionTypeGZIP),
			string(compress.CompressionTypeSNAPPY),
			string(compress.CompressionTypeLZ4),
			string(compress.CompressionTypeS2),
			string(compress.CompressionTypePGZIP),
			string(compress.CompressionTypeZstandard))
	backupCmd.Flag("type",
		fmt.Sprintf("backup type: <%s>/<%s>/<%s>/<%s>",
			defs.PhysicalBackup,
			defs.LogicalBackup,
			defs.IncrementalBackup,
			defs.ExternalBackup)).
		Default(string(defs.LogicalBackup)).
		Short('t').
		EnumVar(&backupOptions.typ,
			string(defs.PhysicalBackup),
			string(defs.LogicalBackup),
			string(defs.IncrementalBackup),
			string(defs.ExternalBackup))
	backupCmd.Flag("base", "Is this a base for incremental backups").
		BoolVar(&backupOptions.base)
	backupCmd.Flag("profile", "Config profile name").StringVar(&backupOptions.profile)
	backupCmd.Flag("compression-level", "Compression level (specific to the compression type)").
		IntsVar(&backupOptions.compressionLevel)
	backupCmd.Flag("num-parallel-collections", "Number of parallel collections").
		Int32Var(&backupOptions.numParallelColls)
	backupCmd.Flag("ns", `Namespaces to backup (e.g. "db.*", "db.collection"). If not set, backup all ("*.*")`).
		StringVar(&backupOptions.ns)
	backupCmd.Flag("wait", "Wait for the backup to finish").
		Short('w').
		BoolVar(&backupOptions.wait)
	backupCmd.Flag("wait-time", "Maximum wait time").
		DurationVar(&backupOptions.waitTime)
	backupCmd.Flag("list-files", "Shows the list of files per node to copy (only for external backups)").
		Short('l').
		BoolVar(&backupOptions.externList)

	cancelBcpCmd := pbmCmd.Command("cancel-backup", "Cancel backup")

	descBcpCmd := pbmCmd.Command("describe-backup", "Describe backup")
	descBcp := descBcp{}
	descBcpCmd.Flag("with-collections", "Show collections in backup").
		BoolVar(&descBcp.coll)
	descBcpCmd.Arg("backup_name", "Backup name").
		StringVar(&descBcp.name)

	finishBackupName := ""
	backupFinishCmd := pbmCmd.Command("backup-finish", "Finish external backup")
	backupFinishCmd.Arg("backup_name", "Backup name").
		StringVar(&finishBackupName)

	finishRestore := descrRestoreOpts{}
	restoreFinishCmd := pbmCmd.Command("restore-finish", "Finish external backup")
	restoreFinishCmd.Arg("restore_name", "Restore name").
		StringVar(&finishRestore.restore)
	restoreFinishCmd.Flag("config", "Path to PBM config").
		Short('c').
		Required().
		StringVar(&finishRestore.cfg)

	restoreCmd := pbmCmd.Command("restore", "Restore backup")
	restore := restoreOpts{}
	restoreCmd.Arg("backup_name", "Backup name to restore").
		StringVar(&restore.bcp)
	restoreCmd.Flag("time", fmt.Sprintf("Restore to the point-in-time. Set in format %s", datetimeFormat)).
		StringVar(&restore.pitr)
	restoreCmd.Flag("base-snapshot",
		"Override setting: Name of older snapshot that PITR will be based on during restore.").
		StringVar(&restore.pitrBase)
	restoreCmd.Flag("num-parallel-collections", "Number of parallel collections").
		Int32Var(&restore.numParallelColls)
	restoreCmd.Flag("num-insertion-workers-per-collection", "Specifies the number of insertion workers to run concurrently per collection. For large imports, increasing the number of insertion workers may increase the speed of the import.").
		Int32Var(&restore.numInsertionWorkers)
	restoreCmd.Flag("ns", `Namespaces to restore (e.g. "db1.*,db2.collection2"). If not set, restore all ("*.*")`).
		StringVar(&restore.ns)
	restoreCmd.Flag("ns-from", "Allows collection cloning (creating from the backup with different name) "+
		"and specifies source collection for cloning from.").
		StringVar(&restore.nsFrom)
	restoreCmd.Flag("ns-to", "Allows collection cloning (creating from the backup with different name) "+
		"and specifies destination collection for cloning to.").
		StringVar(&restore.nsTo)
	restoreCmd.Flag("with-users-and-roles", "Includes users and roles for selected database (--ns flag)").
		BoolVar(&restore.usersAndRoles)
	restoreCmd.Flag("wait", "Wait for the restore to finish.").
		Short('w').
		BoolVar(&restore.wait)
	restoreCmd.Flag("wait-time", "Maximum wait time").
		DurationVar(&restore.waitTime)
	restoreCmd.Flag("external", "External restore.").
		Short('x').
		BoolVar(&restore.extern)
	restoreCmd.Flag("config", "Mongod config for the source data. External backups only!").
		Short('c').
		StringVar(&restore.conf)
	restoreCmd.Flag("ts",
		"MongoDB cluster time to restore to. In <T,I> format (e.g. 1682093090,9). External backups only!").
		StringVar(&restore.ts)
	restoreCmd.Flag(RSMappingFlag, RSMappingDoc).
		Envar(RSMappingEnvVar).
		StringVar(&restore.rsMap)

	replayCmd := pbmCmd.Command("oplog-replay", "Replay oplog")
	replayOpts := replayOptions{}
	replayCmd.Flag("start", fmt.Sprintf("Replay oplog from the time. Set in format %s", datetimeFormat)).
		Required().
		StringVar(&replayOpts.start)
	replayCmd.Flag("end", fmt.Sprintf("Replay oplog to the time. Set in format %s", datetimeFormat)).
		Required().
		StringVar(&replayOpts.end)
	replayCmd.Flag("wait", "Wait for the restore to finish.").
		Short('w').
		BoolVar(&replayOpts.wait)
	replayCmd.Flag("wait-time", "Maximum wait time").
		DurationVar(&replayOpts.waitTime)
	replayCmd.Flag(RSMappingFlag, RSMappingDoc).
		Envar(RSMappingEnvVar).
		StringVar(&replayOpts.rsMap)
	// todo(add oplog cancel)

	listCmd := pbmCmd.Command("list", "Backup list")
	list := listOpts{}
	listCmd.Flag("restore", "Show last N restores").
		Default("false").
		BoolVar(&list.restore)
	listCmd.Flag("unbacked", "Show unbacked oplog ranges").
		Default("false").
		BoolVar(&list.unbacked)
	listCmd.Flag("full", "Show extended restore info").
		Default("false").
		Short('f').
		Hidden().
		BoolVar(&list.full)
	listCmd.Flag("size", "Show last N backups").
		Default("0").
		IntVar(&list.size)
	listCmd.Flag(RSMappingFlag, RSMappingDoc).
		Envar(RSMappingEnvVar).
		StringVar(&list.rsMap)

	deleteBcpCmd := pbmCmd.Command("delete-backup", "Delete a backup")
	deleteBcp := deleteBcpOpts{}
	deleteBcpCmd.Arg("name", "Backup name").
		StringVar(&deleteBcp.name)
	deleteBcpCmd.Flag("older-than",
		fmt.Sprintf("Delete backups older than date/time in format %s or %s",
			datetimeFormat,
			dateFormat)).
		StringVar(&deleteBcp.olderThan)
	deleteBcpCmd.Flag("type",
		fmt.Sprintf("backup type: <%s>/<%s>/<%s>/<%s>/<%s>",
			defs.LogicalBackup,
			defs.PhysicalBackup,
			defs.IncrementalBackup,
			defs.ExternalBackup,
			backup.SelectiveBackup,
		)).
		Short('t').
		EnumVar(&deleteBcp.bcpType,
			string(defs.LogicalBackup),
			string(defs.PhysicalBackup),
			string(defs.IncrementalBackup),
			string(defs.ExternalBackup),
			string(backup.SelectiveBackup),
		)
	deleteBcpCmd.Flag("yes", "Don't ask for confirmation").
		Short('y').
		BoolVar(&deleteBcp.yes)
	deleteBcpCmd.Flag("force", "Don't ask for confirmation (deprecated)").
		Short('f').
		BoolVar(&deleteBcp.yes)
	deleteBcpCmd.Flag("dry-run", "Report but do not delete").
		BoolVar(&deleteBcp.dryRun)

	deletePitrCmd := pbmCmd.Command("delete-pitr", "Delete PITR chunks")
	deletePitr := deletePitrOpts{}
	deletePitrCmd.Flag("older-than",
		fmt.Sprintf("Delete backups older than date/time in format %s or %s",
			datetimeFormat,
			dateFormat)).
		StringVar(&deletePitr.olderThan)
	deletePitrCmd.Flag("all", `Delete all chunks (deprecated). Use --older-than="0d"`).
		Short('a').
		BoolVar(&deletePitr.all)
	deletePitrCmd.Flag("yes", "Don't ask for confirmation").
		Short('y').
		BoolVar(&deletePitr.yes)
	deletePitrCmd.Flag("force", "Don't ask for confirmation (deprecated)").
		Short('f').
		BoolVar(&deletePitr.yes)
	deletePitrCmd.Flag("wait", "Wait for deletion done").
		Short('w').
		BoolVar(&deletePitr.wait)
	deletePitrCmd.Flag("wait-time", "Maximum wait time").
		DurationVar(&deletePitr.waitTime)
	deletePitrCmd.Flag("dry-run", "Report but do not delete").
		BoolVar(&deletePitr.dryRun)

	cleanupCmd := pbmCmd.Command("cleanup", "Delete Backups and PITR chunks")
	cleanupOpts := cleanupOptions{}
	cleanupCmd.Flag("older-than",
		fmt.Sprintf("Delete older than date/time in format %s or %s",
			datetimeFormat,
			dateFormat)).
		StringVar(&cleanupOpts.olderThan)
	cleanupCmd.Flag("yes", "Don't ask for confirmation").
		Short('y').
		BoolVar(&cleanupOpts.yes)
	cleanupCmd.Flag("wait", "Wait for deletion done").
		Short('w').
		BoolVar(&cleanupOpts.wait)
	cleanupCmd.Flag("wait-time", "Maximum wait time").
		DurationVar(&cleanupOpts.waitTime)
	cleanupCmd.Flag("dry-run", "Report but do not delete").
		BoolVar(&cleanupOpts.dryRun)

	logsCmd := pbmCmd.Command("logs", "PBM logs")
	logs := logsOpts{}
	logsCmd.Flag("follow", "Follow output").
		Short('f').
		Default("false").
		BoolVar(&logs.follow)
	logsCmd.Flag("tail", "Show last N entries, 20 entries are shown by default, 0 for all logs").
		Short('t').
		Default("20").
		Int64Var(&logs.tail)
	logsCmd.Flag("node", "Target node in format replset[/host:posrt]").
		Short('n').
		StringVar(&logs.node)
	logsCmd.Flag("severity", "Severity level D, I, W, E or F, low to high. Choosing one includes higher levels too.").
		Short('s').
		Default("I").
		EnumVar(&logs.severity, "D", "I", "W", "E", "F")
	logsCmd.Flag("event",
		"Event in format backup[/2020-10-06T11:45:14Z]. Events: backup, restore, cancelBackup, resync, pitr, delete").
		Short('e').
		StringVar(&logs.event)
	logsCmd.Flag("opid", "Operation ID").
		Short('i').
		StringVar(&logs.opid)
	logsCmd.Flag("timezone",
		"Timezone of log output. `Local`, `UTC` or a location name corresponding to "+
			"a file in the IANA Time Zone database, such as `America/New_York`").
		StringVar(&logs.location)
	logsCmd.Flag("extra", "Show extra data in text format").
		Hidden().
		Short('x').
		BoolVar(&logs.extr)

	statusOpts := statusOptions{}
	statusCmd := pbmCmd.Command("status", "Show PBM status").Alias("s")
	statusCmd.Flag(RSMappingFlag, RSMappingDoc).
		Envar(RSMappingEnvVar).
		StringVar(&statusOpts.rsMap)
	statusCmd.Flag("sections", "Sections of status to display <cluster>/<pitr>/<running>/<backups>.").
		Short('s').
		EnumsVar(&statusOpts.sections, "cluster", "pitr", "running", "backups")
	statusCmd.Flag("priority", "Show backup and PITR priorities").
		Short('p').
		Default("false").
		BoolVar(&statusOpts.priority)

	describeRestoreCmd := pbmCmd.Command("describe-restore", "Describe restore")
	describeRestoreOpts := descrRestoreOpts{}
	describeRestoreCmd.Arg("name", "Restore name").
		StringVar(&describeRestoreOpts.restore)
	describeRestoreCmd.Flag("config", "Path to PBM config").
		Short('c').
		StringVar(&describeRestoreOpts.cfg)

	cmd, err := pbmCmd.DefaultEnvars().Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error: parse command line parameters:", err)
		os.Exit(1)
	}
	pbmOutF := outFormat(*pbmOutFormat)
	var out fmt.Stringer

	if cmd == versionCmd.FullCommand() {
		switch {
		case *versionCommit:
			out = outCaption{"GitCommit", version.Current().GitCommit}
		case *versionShort:
			out = outCaption{"Version", version.Current().Version}
		default:
			out = version.Current()
		}
		printo(out, pbmOutF)
		return
	}

	if *mURL == "" {
		fmt.Fprintln(os.Stderr, "Error: no mongodb connection URI supplied")
		fmt.Fprintln(os.Stderr,
			"       Usual practice is the set it by the PBM_MONGODB_URI environment variable. "+
				"It can also be set with commandline argument --mongodb-uri.")
		pbmCmd.Usage(os.Args[1:])
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var conn connect.Client
	var pbm *sdk.Client
	var node string
	// we don't need pbm connection if it is `pbm describe-restore -c ...`
	// or `pbm restore-finish `
	if describeRestoreOpts.cfg == "" && finishRestore.cfg == "" {
		conn, err = connect.Connect(ctx, *mURL, "pbm-ctl")
		if err != nil {
			exitErr(errors.Wrap(err, "connect to mongodb"), pbmOutF)
		}
		ctx = log.SetLoggerToContext(ctx, log.New(conn.LogCollection(), "", ""))

		ver, err := version.GetMongoVersion(ctx, conn.MongoClient())
		if err != nil {
			fmt.Fprintf(os.Stderr, "get mongo version: %v", err)
			os.Exit(1)
		}
		if err := version.FeatureSupport(ver).PBMSupport(); err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: %v\n", err)
		}

		pbm, err = sdk.NewClient(ctx, *mURL)
		if err != nil {
			exitErr(errors.Wrap(err, "init sdk"), pbmOutF)
		}
		defer pbm.Close(context.Background())

		inf, err := topo.GetNodeInfo(ctx, conn.MongoClient())
		if err != nil {
			exitErr(errors.Wrap(err, "unable to obtain node info"), pbmOutF)
		}
		node = inf.Me
	}

	switch cmd {
	case configCmd.FullCommand():
		out, err = runConfig(ctx, conn, pbm, &cfg)
	case listConfigProfileCmd.FullCommand():
		out, err = handleListConfigProfiles(ctx, pbm)
	case showConfigProfileCmd.FullCommand():
		out, err = handleShowConfigProfiles(ctx, pbm, showConfigProfileOpts)
	case addConfigProfileCmd.FullCommand():
		out, err = handleAddConfigProfile(ctx, pbm, addConfigProfileOpts)
	case removeConfigProfileCmd.FullCommand():
		out, err = handleRemoveConfigProfile(ctx, pbm, removeConfigProfileOpts)
	case syncConfigProfileCmd.FullCommand():
		out, err = handleSyncConfigProfile(ctx, pbm, syncConfigProfileOpts)
	case backupCmd.FullCommand():
		backupOptions.name = time.Now().UTC().Format(time.RFC3339)
		out, err = runBackup(ctx, conn, pbm, &backupOptions, pbmOutF)
	case cancelBcpCmd.FullCommand():
		out, err = cancelBcp(ctx, pbm)
	case backupFinishCmd.FullCommand():
		out, err = runFinishBcp(ctx, conn, finishBackupName)
	case restoreFinishCmd.FullCommand():
		out, err = runFinishRestore(finishRestore, node)
	case descBcpCmd.FullCommand():
		out, err = describeBackup(ctx, pbm, &descBcp, node)
	case restoreCmd.FullCommand():
		out, err = runRestore(ctx, conn, pbm, &restore, node, pbmOutF)
	case replayCmd.FullCommand():
		out, err = replayOplog(ctx, conn, pbm, replayOpts, node, pbmOutF)
	case listCmd.FullCommand():
		out, err = runList(ctx, conn, pbm, &list)
	case deleteBcpCmd.FullCommand():
		out, err = deleteBackup(ctx, conn, pbm, &deleteBcp)
	case deletePitrCmd.FullCommand():
		out, err = deletePITR(ctx, conn, pbm, &deletePitr)
	case cleanupCmd.FullCommand():
		out, err = doCleanup(ctx, conn, pbm, &cleanupOpts)
	case logsCmd.FullCommand():
		out, err = runLogs(ctx, conn, &logs, pbmOutF)
	case statusCmd.FullCommand():
		out, err = status(ctx, conn, pbm, *mURL, statusOpts, pbmOutF == outJSONpretty)
	case describeRestoreCmd.FullCommand():
		out, err = describeRestore(ctx, conn, describeRestoreOpts, node)
	}

	if err != nil {
		exitErr(err, pbmOutF)
	}

	printo(out, pbmOutF)

	if r, ok := out.(cliResult); ok && r.HasError() {
		os.Exit(1)
	}
}

func printo(out fmt.Stringer, f outFormat) {
	if out == nil {
		return
	}

	switch f {
	case outJSON:
		err := json.NewEncoder(os.Stdout).Encode(out)
		if err != nil {
			exitErr(errors.Wrap(err, "encode output"), f)
		}
	case outJSONpretty:
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		err := enc.Encode(out)
		if err != nil {
			exitErr(errors.Wrap(err, "encode output"), f)
		}
	default:
		fmt.Println(strings.TrimSpace(out.String()))
	}
}

func exitErr(e error, f outFormat) {
	switch f {
	case outJSON, outJSONpretty:
		var m interface{}
		m = e
		if _, ok := e.(json.Marshaler); !ok { //nolint:errorlint
			m = map[string]string{"Error": e.Error()}
		}

		j := json.NewEncoder(os.Stdout)
		if f == outJSONpretty {
			j.SetIndent("", "    ")
		}

		if err := j.Encode(m); err != nil {
			fmt.Fprintf(os.Stderr, "Error: encoding error \"%v\": %v", m, err)
		}
	default:
		fmt.Fprintln(os.Stderr, "Error:", e)
	}

	os.Exit(1)
}

func runLogs(ctx context.Context, conn connect.Client, l *logsOpts, f outFormat) (fmt.Stringer, error) {
	r := &log.LogRequest{}

	if l.node != "" {
		n := strings.Split(l.node, "/")
		r.RS = n[0]
		if len(n) > 1 {
			r.Node = n[1]
		}
	}

	if l.event != "" {
		e := strings.Split(l.event, "/")
		r.Event = e[0]
		if len(e) > 1 {
			r.ObjName = e[1]
		}
	}

	if l.opid != "" {
		r.OPID = l.opid
	}

	switch l.severity {
	case "F":
		r.Severity = log.Fatal
	case "E":
		r.Severity = log.Error
	case "W":
		r.Severity = log.Warning
	case "I":
		r.Severity = log.Info
	case "D":
		r.Severity = log.Debug
	default:
		r.Severity = log.Info
	}

	if l.follow {
		err := followLogs(ctx, conn, r, r.Node == "", l.extr, f)
		return nil, err
	}

	o, err := log.LogGet(ctx, conn, r, l.tail)
	if err != nil {
		return nil, errors.Wrap(err, "get logs")
	}

	o.ShowNode = r.Node == ""
	o.Extr = l.extr

	// reverse list
	for i := len(o.Data)/2 - 1; i >= 0; i-- {
		opp := len(o.Data) - 1 - i
		o.Data[i], o.Data[opp] = o.Data[opp], o.Data[i]
	}

	err = o.SetLocation(l.location)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to parse timezone: %v\n\n", err)
	}

	return o, nil
}

func followLogs(ctx context.Context, conn connect.Client, r *log.LogRequest, showNode, expr bool, f outFormat) error {
	outC, errC := log.Follow(ctx, conn, r, false)

	var enc *json.Encoder
	switch f {
	case outJSON:
		enc = json.NewEncoder(os.Stdout)
	case outJSONpretty:
		enc = json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
	}

	for {
		select {
		case entry, ok := <-outC:
			if !ok {
				return nil
			}

			if f == outJSON || f == outJSONpretty {
				err := enc.Encode(entry)
				if err != nil {
					exitErr(errors.Wrap(err, "encode output"), f)
				}
			} else {
				fmt.Println(entry.Stringify(tsUTC, showNode, expr))
			}
		case err, ok := <-errC:
			if !ok {
				return nil
			}

			return err
		}
	}
}

func tsUTC(ts int64) string {
	return time.Unix(ts, 0).UTC().Format(time.RFC3339)
}

type snapshotStat struct {
	Name       string          `json:"name"`
	Namespaces []string        `json:"nss,omitempty"`
	Size       int64           `json:"size,omitempty"`
	Status     defs.Status     `json:"status"`
	Err        error           `json:"-"`
	ErrString  string          `json:"error,omitempty"`
	RestoreTS  int64           `json:"restoreTo"`
	PBMVersion string          `json:"pbmVersion"`
	Type       defs.BackupType `json:"type"`
	SrcBackup  string          `json:"src"`
	StoreName  string          `json:"storage,omitempty"`
}

type pitrRange struct {
	Err            error          `json:"error,omitempty"`
	Range          oplog.Timeline `json:"range"`
	NoBaseSnapshot bool           `json:"noBaseSnapshot,omitempty"`
}

func (pr pitrRange) String() string {
	return fmt.Sprintf("{ %s }", pr.Range)
}

func fmtTS(ts int64) string {
	return time.Unix(ts, 0).UTC().Format(time.RFC3339)
}

type outMsg struct {
	Msg string `json:"msg"`
}

func (m outMsg) String() string {
	return m.Msg
}

type outCaption struct {
	k string
	v interface{}
}

func (c outCaption) String() string {
	return fmt.Sprint(c.v)
}

func (c outCaption) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteString("{")
	b.WriteString(fmt.Sprintf("\"%s\":", c.k))
	err := json.NewEncoder(&b).Encode(c.v)
	if err != nil {
		return nil, err
	}
	b.WriteString("}")
	return b.Bytes(), nil
}

func cancelBcp(ctx context.Context, pbm *sdk.Client) (fmt.Stringer, error) {
	if _, err := pbm.CancelBackup(ctx); err != nil {
		return nil, errors.Wrap(err, "send backup canceling")
	}
	return outMsg{"Backup cancellation has started"}, nil
}

var errInvalidFormat = errors.New("invalid format")

func parseDateT(v string) (time.Time, error) {
	switch len(v) {
	case len(datetimeFormat):
		return time.Parse(datetimeFormat, v)
	case len(dateFormat):
		return time.Parse(dateFormat, v)
	}

	return time.Time{}, errInvalidFormat
}
