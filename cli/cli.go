package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/version"
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

func Main() {
	var (
		pbmCmd       = kingpin.New("pbm", "Percona Backup for MongoDB")
		mURL         = pbmCmd.Flag("mongodb-uri", "MongoDB connection string (Default = PBM_MONGODB_URI environment variable)").Envar("PBM_MONGODB_URI").String()
		pbmOutFormat = pbmCmd.Flag("out", "Output format <text>/<json>").Short('o').Default(string(outText)).Enum(string(outJSON), string(outJSONpretty), string(outText))
	)
	pbmCmd.HelpFlag.Short('h')

	versionCmd := pbmCmd.Command("version", "PBM version info")
	versionShort := versionCmd.Flag("short", "Show only version info").Short('s').Default("false").Bool()
	versionCommit := versionCmd.Flag("commit", "Show only git commit info").Short('c').Default("false").Bool()

	configCmd := pbmCmd.Command("config", "Set, change or list the config")
	cfg := configOpts{set: make(map[string]string)}
	configCmd.Flag("force-resync", "Resync backup list with the current store").BoolVar(&cfg.rsync)
	configCmd.Flag("list", "List current settings").BoolVar(&cfg.list)
	configCmd.Flag("file", "Upload config from YAML file").StringVar(&cfg.file)
	configCmd.Flag("set", "Set the option value <key.name=value>").StringMapVar(&cfg.set)
	configCmd.Arg("key", "Show the value of a specified key").StringVar(&cfg.key)

	backupCmd := pbmCmd.Command("backup", "Make backup")
	backup := backupOpts{}
	backupCmd.Flag("compression", "Compression type <none>/<gzip>/<snappy>/<lz4>/<s2>/<pgzip>/<zstd>").
		EnumVar(&backup.compression,
			string(compress.CompressionTypeNone), string(compress.CompressionTypeGZIP),
			string(compress.CompressionTypeSNAPPY), string(compress.CompressionTypeLZ4),
			string(compress.CompressionTypeS2), string(compress.CompressionTypePGZIP),
			string(compress.CompressionTypeZstandard),
		)
	backupCmd.Flag("type",
		fmt.Sprintf("backup type: <%s>/<%s>/<%s>/<%s>",
			pbm.PhysicalBackup, pbm.LogicalBackup, pbm.IncrementalBackup, pbm.ExternalBackup)).
		Default(string(pbm.LogicalBackup)).Short('t').
		EnumVar(&backup.typ,
			string(pbm.PhysicalBackup),
			string(pbm.LogicalBackup),
			string(pbm.IncrementalBackup),
			string(pbm.ExternalBackup),
		)
	backupCmd.Flag("base", "Is this a base for incremental backups").BoolVar(&backup.base)
	backupCmd.Flag("compression-level", "Compression level (specific to the compression type)").
		IntsVar(&backup.compressionLevel)
	backupCmd.Flag("ns", `Namespaces to backup (e.g. "db.*", "db.collection"). If not set, backup all ("*.*")`).StringVar(&backup.ns)
	backupCmd.Flag("wait", "Wait for the backup to finish").Short('w').BoolVar(&backup.wait)
	backupCmd.Flag("list-files", "Wait for the backup to finish").Short('l').BoolVar(&backup.externList)

	cancelBcpCmd := pbmCmd.Command("cancel-backup", "Cancel backup")

	descBcpCmd := pbmCmd.Command("describe-backup", "Describe backup")
	descBcp := descBcp{}
	descBcpCmd.Arg("backup_name", "Backup name").StringVar(&descBcp.name)

	finishBackupName := ""
	backupFinishCmd := pbmCmd.Command("backup-finish", "Finish external backup")
	backupFinishCmd.Arg("backup_name", "Backup name").StringVar(&finishBackupName)

	finishRestore := descrRestoreOpts{}
	restoreFinishCmd := pbmCmd.Command("restore-finish", "Finish external backup")
	restoreFinishCmd.Arg("restore_name", "Restore name").StringVar(&finishRestore.restore)
	restoreFinishCmd.Flag("config", "Path to PBM config").Short('c').Required().StringVar(&finishRestore.cfg)

	restoreCmd := pbmCmd.Command("restore", "Restore backup")
	restore := restoreOpts{}
	restoreCmd.Arg("backup_name", "Backup name to restore").StringVar(&restore.bcp)
	restoreCmd.Flag("time", fmt.Sprintf("Restore to the point-in-time. Set in format %s", datetimeFormat)).StringVar(&restore.pitr)
	restoreCmd.Flag("base-snapshot", "Override setting: Name of older snapshot that PITR will be based on during restore.").StringVar(&restore.pitrBase)
	restoreCmd.Flag("ns", `Namespaces to restore (e.g. "db1.*,db2.collection2"). If not set, restore all ("*.*")`).StringVar(&restore.ns)
	restoreCmd.Flag("wait", "Wait for the restore to finish.").Short('w').BoolVar(&restore.wait)
	restoreCmd.Flag("external", "External restore.").Short('x').BoolVar(&restore.extern)
	restoreCmd.Flag("config", "Mongod config for the source data. External backups only!").Short('c').StringVar(&restore.conf)
	restoreCmd.Flag("ts", "MongoDB cluster time to restore to. In <T,I> format (e.g. 1682093090,9). External backups only!").StringVar(&restore.ts)
	restoreCmd.Flag(RSMappingFlag, RSMappingDoc).Envar(RSMappingEnvVar).StringVar(&restore.rsMap)

	replayCmd := pbmCmd.Command("oplog-replay", "Replay oplog")
	replayOpts := replayOptions{}
	replayCmd.Flag("start", fmt.Sprintf("Replay oplog from the time. Set in format %s", datetimeFormat)).Required().StringVar(&replayOpts.start)
	replayCmd.Flag("end", "Replay oplog to the time. Set in format %s").Required().StringVar(&replayOpts.end)
	replayCmd.Flag("wait", "Wait for the restore to finish.").Short('w').BoolVar(&replayOpts.wait)
	replayCmd.Flag(RSMappingFlag, RSMappingDoc).Envar(RSMappingEnvVar).StringVar(&replayOpts.rsMap)
	// todo(add oplog cancel)

	listCmd := pbmCmd.Command("list", "Backup list")
	list := listOpts{}
	listCmd.Flag("restore", "Show last N restores").Default("false").BoolVar(&list.restore)
	listCmd.Flag("unbacked", "Show unbacked oplog ranges").Default("false").BoolVar(&list.unbacked)
	listCmd.Flag("full", "Show extended restore info").Default("false").Short('f').Hidden().BoolVar(&list.full)
	listCmd.Flag("size", "Show last N backups").Default("0").IntVar(&list.size)
	listCmd.Flag(RSMappingFlag, RSMappingDoc).Envar(RSMappingEnvVar).StringVar(&list.rsMap)

	deleteBcpCmd := pbmCmd.Command("delete-backup", "Delete a backup")
	deleteBcp := deleteBcpOpts{}
	deleteBcpCmd.Arg("name", "Backup name").StringVar(&deleteBcp.name)
	deleteBcpCmd.Flag("older-than", fmt.Sprintf("Delete backups older than date/time in format %s or %s", datetimeFormat, dateFormat)).StringVar(&deleteBcp.olderThan)
	deleteBcpCmd.Flag("yes", "Don't ask confirmation").Short('y').BoolVar(&deleteBcp.force)
	deleteBcpCmd.Flag("force", "Force. Don't ask confirmation").Short('f').BoolVar(&deleteBcp.force)

	deletePitrCmd := pbmCmd.Command("delete-pitr", "Delete PITR chunks")
	deletePitr := deletePitrOpts{}
	deletePitrCmd.Flag("older-than", fmt.Sprintf("Delete backups older than date/time in format %s or %s", datetimeFormat, dateFormat)).StringVar(&deletePitr.olderThan)
	deletePitrCmd.Flag("all", "Delete all chunks").Short('a').BoolVar(&deletePitr.all)
	deletePitrCmd.Flag("yes", "Don't ask confirmation").Short('y').BoolVar(&deletePitr.force)
	deletePitrCmd.Flag("force", "Force. Don't ask confirmation").Short('f').BoolVar(&deletePitr.force)

	cleanupCmd := pbmCmd.Command("cleanup", "Delete Backups and PITR chunks")
	cleanupOpts := cleanupOptions{}
	cleanupCmd.Flag("older-than", fmt.Sprintf("Delete older than date/time in format %s or %s", datetimeFormat, dateFormat)).StringVar(&cleanupOpts.olderThan)
	cleanupCmd.Flag("yes", "Don't ask confirmation").Short('y').BoolVar(&cleanupOpts.yes)
	cleanupCmd.Flag("wait", "Wait for deletion done").Short('w').BoolVar(&cleanupOpts.wait)
	cleanupCmd.Flag("dry-run", "Report but do not delete").BoolVar(&cleanupOpts.dryRun)

	logsCmd := pbmCmd.Command("logs", "PBM logs")
	logs := logsOpts{}
	logsCmd.Flag("follow", "Follow output").Short('f').Default("false").BoolVar(&logs.follow)
	logsCmd.Flag("tail", "Show last N entries, 20 entries are shown by default, 0 for all logs").Short('t').Default("20").Int64Var(&logs.tail)
	logsCmd.Flag("node", "Target node in format replset[/host:posrt]").Short('n').StringVar(&logs.node)
	logsCmd.Flag("severity", "Severity level D, I, W, E or F, low to high. Choosing one includes higher levels too.").Short('s').Default("I").EnumVar(&logs.severity, "D", "I", "W", "E", "F")
	logsCmd.Flag("event", "Event in format backup[/2020-10-06T11:45:14Z]. Events: backup, restore, cancelBackup, resync, pitr, pitrestore, delete").Short('e').StringVar(&logs.event)
	logsCmd.Flag("opid", "Operation ID").Short('i').StringVar(&logs.opid)
	logsCmd.Flag("timezone", "Timezone of log output. `Local`, `UTC` or a location name corresponding to a file in the IANA Time Zone database, such as `America/New_York`").StringVar(&logs.location)
	logsCmd.Flag("extra", "Show extra data in text format").Hidden().Short('x').BoolVar(&logs.extr)

	statusOpts := statusOptions{}
	statusCmd := pbmCmd.Command("status", "Show PBM status")
	statusCmd.Flag(RSMappingFlag, RSMappingDoc).Envar(RSMappingEnvVar).StringVar(&statusOpts.rsMap)
	statusCmd.Flag("sections", "Sections of status to display <cluster>/<pitr>/<running>/<backups>.").Short('s').
		EnumsVar(&statusOpts.sections, "cluster", "pitr", "running", "backups")

	describeRestoreCmd := pbmCmd.Command("describe-restore", "Describe restore")
	describeRestoreOpts := descrRestoreOpts{}
	describeRestoreCmd.Arg("name", "Restore name").StringVar(&describeRestoreOpts.restore)
	describeRestoreCmd.Flag("config", "Path to PBM config").Short('c').StringVar(&describeRestoreOpts.cfg)

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
			out = outCaption{"GitCommit", version.DefaultInfo.GitCommit}
		case *versionShort:
			out = outCaption{"Version", version.DefaultInfo.Version}
		default:
			out = version.DefaultInfo
		}
		printo(out, pbmOutF)
		return
	}

	if *mURL == "" {
		fmt.Fprintln(os.Stderr, "Error: no mongodb connection URI supplied")
		fmt.Fprintln(os.Stderr, "       Usual practice is the set it by the PBM_MONGODB_URI environment variable. It can also be set with commandline argument --mongodb-uri.")
		pbmCmd.Usage(os.Args[1:])
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pbmClient *pbm.PBM
	// we don't need pbm connection if it is `pbm describe-restore -c ...`
	// or `pbm restore-finish `
	if describeRestoreOpts.cfg == "" && finishRestore.cfg == "" {
		pbmClient, err = pbm.New(ctx, *mURL, "pbm-ctl")
		if err != nil {
			exitErr(errors.Wrap(err, "connect to mongodb"), pbmOutF)
		}
		pbmClient.InitLogger("", "")
	}

	switch cmd {
	case configCmd.FullCommand():
		out, err = runConfig(pbmClient, &cfg)
	case backupCmd.FullCommand():
		backup.name = time.Now().UTC().Format(time.RFC3339)
		out, err = runBackup(pbmClient, &backup, pbmOutF)
	case cancelBcpCmd.FullCommand():
		out, err = cancelBcp(pbmClient)
	case backupFinishCmd.FullCommand():
		out, err = runFinishBcp(pbmClient, finishBackupName)
	case restoreFinishCmd.FullCommand():
		out, err = runFinishRestore(finishRestore)
	case descBcpCmd.FullCommand():
		out, err = describeBackup(pbmClient, &descBcp)
	case restoreCmd.FullCommand():
		out, err = runRestore(pbmClient, &restore, pbmOutF)
	case replayCmd.FullCommand():
		out, err = replayOplog(pbmClient, replayOpts, pbmOutF)
	case listCmd.FullCommand():
		out, err = runList(pbmClient, &list)
	case deleteBcpCmd.FullCommand():
		out, err = deleteBackup(pbmClient, &deleteBcp, pbmOutF)
	case deletePitrCmd.FullCommand():
		out, err = deletePITR(pbmClient, &deletePitr, pbmOutF)
	case cleanupCmd.FullCommand():
		out, err = retentionCleanup(pbmClient, &cleanupOpts)
	case logsCmd.FullCommand():
		out, err = runLogs(pbmClient, &logs)
	case statusCmd.FullCommand():
		out, err = status(pbmClient, *mURL, statusOpts, pbmOutF == outJSONpretty)
	case describeRestoreCmd.FullCommand():
		out, err = describeRestore(pbmClient, describeRestoreOpts)
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
		if _, ok := e.(json.Marshaler); !ok {
			m = map[string]string{"Error": e.Error()}
		}
		var err error
		if f == outJSONpretty {
			err = json.NewEncoder(os.Stdout).Encode(m)
		} else {
			err = json.NewEncoder(os.Stdout).Encode(m)
		}

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: encoding error \"%v\": %v", m, err)
		}
	default:
		fmt.Fprintln(os.Stderr, "Error:", e)
	}

	os.Exit(1)
}

func runLogs(cn *pbm.PBM, l *logsOpts) (fmt.Stringer, error) {
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
		err := followLogs(cn, r, r.Node == "", l.extr)
		return nil, err
	}

	o, err := cn.LogGet(r, l.tail)
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

func followLogs(cn *pbm.PBM, r *log.LogRequest, showNode, expr bool) error {
	outC, errC := log.Follow(cn.Context(), cn.Conn.Database(pbm.DB).Collection(pbm.LogCollection), r, false)

	for {
		select {
		case entry, ok := <-outC:
			if !ok {
				return nil
			}

			fmt.Println(entry.Stringify(tsUTC, showNode, expr))
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
	Name       string         `json:"name"`
	Namespaces []string       `json:"nss,omitempty"`
	Size       int64          `json:"size,omitempty"`
	Status     pbm.Status     `json:"status"`
	Err        error          `json:"-"`
	ErrString  string         `json:"error,omitempty"`
	RestoreTS  int64          `json:"restoreTo"`
	PBMVersion string         `json:"pbmVersion"`
	Type       pbm.BackupType `json:"type"`
	SrcBackup  string         `json:"src"`
}

type pitrRange struct {
	Err            error        `json:"error,omitempty"`
	Range          pbm.Timeline `json:"range"`
	NoBaseSnapshot bool         `json:"noBaseSnapshot,omitempty"`
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

func (m outMsg) String() (s string) {
	return m.Msg
}

type outCaption struct {
	k string
	v interface{}
}

func (c outCaption) String() (s string) {
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

func cancelBcp(cn *pbm.PBM) (fmt.Stringer, error) {
	err := cn.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdCancelBackup,
	})
	if err != nil {
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

func findLock(cn *pbm.PBM, fn func(*pbm.LockHeader) ([]pbm.LockData, error)) (*pbm.LockData, error) {
	locks, err := fn(&pbm.LockHeader{})
	if err != nil {
		return nil, errors.Wrap(err, "get locks")
	}

	ct, err := cn.ClusterTime()
	if err != nil {
		return nil, errors.Wrap(err, "get cluster time")
	}

	var lk *pbm.LockData
	for _, l := range locks {
		// We don't care about the PITR slicing here. It is a subject of other status sections
		if l.Type == pbm.CmdPITR || l.Heartbeat.T+pbm.StaleFrameSec < ct.T {
			continue
		}

		// Just check if all locks are for the same op
		//
		// It could happen that the healthy `lk` became stale by the time of this check
		// or the op was finished and the new one was started. So the `l.Type != lk.Type`
		// would be true but for the legit reason (no error).
		// But chances for that are quite low and on the next run of `pbm status` everything
		//  would be ok. So no reason to complicate code to avoid that.
		if lk != nil && l.OPID != lk.OPID {
			if err != nil {
				return nil, errors.Errorf("conflicting ops running: [%s/%s::%s-%s] [%s/%s::%s-%s]. This conflict may naturally resolve after 10 seconds",
					l.Replset, l.Node, l.Type, l.OPID,
					lk.Replset, lk.Node, lk.Type, lk.OPID,
				)
			}
		}

		lk = &l
	}

	return lk, nil
}

var errTout = errors.Errorf("timeout reached")

// waitOp waits up to waitFor duration until operations which acquires a given lock are finished
func waitOp(pbmClient *pbm.PBM, lock *pbm.LockHeader, waitFor time.Duration) error {
	// just to be sure the check hasn't started before the lock were created
	time.Sleep(1 * time.Second)
	fmt.Print(".")

	tmr := time.NewTimer(waitFor)
	defer tmr.Stop()
	tkr := time.NewTicker(1 * time.Second)
	defer tkr.Stop()
	for {
		select {
		case <-tmr.C:
			return errTout
		case <-tkr.C:
			fmt.Print(".")
			lock, err := pbmClient.GetLockData(lock)
			if err != nil {
				// No lock, so operation has finished
				if err == mongo.ErrNoDocuments {
					return nil
				}
				return errors.Wrap(err, "get lock data")
			}
			clusterTime, err := pbmClient.ClusterTime()
			if err != nil {
				return errors.Wrap(err, "read cluster time")
			}
			if lock.Heartbeat.T+pbm.StaleFrameSec < clusterTime.T {
				return errors.Errorf("operation stale, last beat ts: %d", lock.Heartbeat.T)
			}
		}
	}
}

func isTTY() bool {
	fi, err := os.Stdin.Stat()
	return (fi.Mode()&os.ModeCharDevice) != 0 && err == nil
}

func lastLogErr(cn *pbm.PBM, op pbm.Command, after int64) (string, error) {
	l, err := cn.LogGet(
		&log.LogRequest{
			LogKeys: log.LogKeys{
				Severity: log.Error,
				Event:    string(op),
			},
		}, 1)
	if err != nil {
		return "", errors.Wrap(err, "get log records")
	}
	if len(l.Data) == 0 {
		return "", nil
	}

	if l.Data[0].TS < after {
		return "", nil
	}

	return l.Data[0].Msg, nil
}

type concurentOpErr struct {
	op *pbm.LockHeader
}

func (e concurentOpErr) Error() string {
	return fmt.Sprintf("another operation in progress, %s/%s [%s/%s]", e.op.Type, e.op.OPID, e.op.Replset, e.op.Node)
}

func (e concurentOpErr) MarshalJSON() ([]byte, error) {
	s := make(map[string]interface{})
	s["error"] = "another operation in progress"
	s["operation"] = e.op
	return json.Marshal(s)
}

func checkConcurrentOp(cn *pbm.PBM) error {
	locks, err := cn.GetLocks(&pbm.LockHeader{})
	if err != nil {
		return errors.Wrap(err, "get locks")
	}

	ts, err := cn.ClusterTime()
	if err != nil {
		return errors.Wrap(err, "read cluster time")
	}

	// Stop if there is some live operation.
	// But in case of stale lock just move on
	// and leave it for agents to deal with.
	for _, l := range locks {
		if l.Heartbeat.T+pbm.StaleFrameSec >= ts.T {
			return concurentOpErr{&l.LockHeader}
		}
	}

	return nil
}
