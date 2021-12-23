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
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/version"
)

const (
	datetimeFormat = "2006-01-02T15:04:05"
	dateFormat     = "2006-01-02"
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
	extr     bool
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
	backupCmd.Flag("compression", "Compression type <none>/<gzip>/<snappy>/<lz4>/<s2>/<pgzip>").
		Default(string(pbm.CompressionTypeS2)).
		EnumVar(&backup.compression,
			string(pbm.CompressionTypeNone), string(pbm.CompressionTypeGZIP),
			string(pbm.CompressionTypeSNAPPY), string(pbm.CompressionTypeLZ4),
			string(pbm.CompressionTypeS2), string(pbm.CompressionTypePGZIP),
			string(pbm.CompressionTypeZstandard),
		)
	backupCmd.Flag("compression-level", "Compression level (specific to the compression type)").
		IntsVar(&backup.compressionLevel)

	cancelBcpCmd := pbmCmd.Command("cancel-backup", "Cancel backup")

	restoreCmd := pbmCmd.Command("restore", "Restore backup")
	restore := restoreOpts{}
	restoreCmd.Arg("backup_name", "Backup name to restore").StringVar(&restore.bcp)
	restoreCmd.Flag("time", fmt.Sprintf("Restore to the point-in-time. Set in format %s", datetimeFormat)).StringVar(&restore.pitr)
	restoreCmd.Flag("base-snapshot", "Override setting: Name of older snapshot that PITR will be based on during restore.").StringVar(&restore.pitrBase)

	listCmd := pbmCmd.Command("list", "Backup list")
	list := listOpts{}
	listCmd.Flag("restore", "Show last N restores").Default("false").BoolVar(&list.restore)
	listCmd.Flag("full", "Show extended restore info").Default("false").Short('f').Hidden().BoolVar(&list.full)
	listCmd.Flag("size", "Show last N backups").Default("0").IntVar(&list.size)

	deleteBcpCmd := pbmCmd.Command("delete-backup", "Delete a backup")
	deleteBcp := deleteBcpOpts{}
	deleteBcpCmd.Arg("name", "Backup name").StringVar(&deleteBcp.name)
	deleteBcpCmd.Flag("older-than", fmt.Sprintf("Delete backups older than date/time in format %s or %s", datetimeFormat, dateFormat)).StringVar(&deleteBcp.olderThan)
	deleteBcpCmd.Flag("force", "Force. Don't ask confirmation").Short('f').BoolVar(&deleteBcp.force)

	deletePitrCmd := pbmCmd.Command("delete-pitr", "Delete PITR chunks")
	deletePitr := deletePitrOpts{}
	deletePitrCmd.Flag("older-than", fmt.Sprintf("Delete backups older than date/time in format %s or %s", datetimeFormat, dateFormat)).StringVar(&deletePitr.olderThan)
	deletePitrCmd.Flag("all", "Delete all chunks").Short('a').BoolVar(&deletePitr.all)
	deletePitrCmd.Flag("force", "Force. Don't ask confirmation").Short('f').BoolVar(&deletePitr.force)

	logsCmd := pbmCmd.Command("logs", "PBM logs")
	logs := logsOpts{}
	logsCmd.Flag("tail", "Show last N entries, 20 entries are shown by default, 0 for all logs").Short('t').Default("20").Int64Var(&logs.tail)
	logsCmd.Flag("node", "Target node in format replset[/host:posrt]").Short('n').StringVar(&logs.node)
	logsCmd.Flag("severity", "Severity level D, I, W, E or F, low to high. Choosing one includes higher levels too.").Short('s').Default("I").EnumVar(&logs.severity, "D", "I", "W", "E", "F")
	logsCmd.Flag("event", "Event in format backup[/2020-10-06T11:45:14Z]. Events: backup, restore, cancelBackup, resyncBcpList, pitr, pitrestore, delete").Short('e').StringVar(&logs.event)
	logsCmd.Flag("opid", "Operation ID").Short('i').StringVar(&logs.opid)
	logsCmd.Flag("extra", "Show extra data in text format").Hidden().Short('x').BoolVar(&logs.extr)

	statusCmd := pbmCmd.Command("status", "Show PBM status")
	statusSection := statusCmd.Flag("sections", "Sections of status to display <cluster>/<pitr>/<running>/<backups>.").Short('s').Enums("cluster", "pitr", "running", "backups")

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

	pbmClient, err := pbm.New(ctx, *mURL, "pbm-ctl")
	if err != nil {
		exitErr(errors.Wrap(err, "connect to mongodb"), pbmOutF)
	}

	switch cmd {
	case configCmd.FullCommand():
		out, err = runConfig(pbmClient, &cfg)
	case backupCmd.FullCommand():
		backup.name = time.Now().UTC().Format(time.RFC3339)
		out, err = runBackup(pbmClient, &backup, pbmOutF)
	case cancelBcpCmd.FullCommand():
		out, err = cancelBcp(pbmClient)
	case restoreCmd.FullCommand():
		out, err = runRestore(pbmClient, &restore, pbmOutF)
	case listCmd.FullCommand():
		out, err = runList(pbmClient, &list)
	case deleteBcpCmd.FullCommand():
		out, err = deleteBackup(pbmClient, &deleteBcp, pbmOutF)
	case deletePitrCmd.FullCommand():
		out, err = deletePITR(pbmClient, &deletePitr, pbmOutF)
	case logsCmd.FullCommand():
		out, err = runLogs(pbmClient, &logs)
	case statusCmd.FullCommand():
		out, err = status(pbmClient, *mURL, statusSection, pbmOutF == outJSONpretty)
	}

	if err != nil {
		exitErr(err, pbmOutF)
	}

	printo(out, pbmOutF)
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
			m = map[string]error{"Error": e}
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
	r := &plog.LogRequest{}

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
		r.Severity = plog.Fatal
	case "E":
		r.Severity = plog.Error
	case "W":
		r.Severity = plog.Warning
	case "I":
		r.Severity = plog.Info
	case "D":
		r.Severity = plog.Debug
	default:
		r.Severity = plog.Info
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

	return o, nil
}

type snapshotStat struct {
	Name       string     `json:"name"`
	Size       int64      `json:"size,omitempty"`
	Status     pbm.Status `json:"status"`
	Err        string     `json:"error,omitempty"`
	StateTS    int64      `json:"completeTS"`
	PBMVersion string     `json:"pbmVersion"`
}

type pitrRange struct {
	Err   string       `json:"error,omitempty"`
	Range pbm.Timeline `json:"range"`
}

func fmtTS(ts int64) string {
	return time.Unix(ts, 0).UTC().Format("2006-01-02T15:04:05")
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
	json.NewEncoder(&b).Encode(c.v)
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

func parseDateT(v string) (time.Time, error) {
	switch len(v) {
	case len(datetimeFormat):
		return time.Parse(datetimeFormat, v)
	case len(dateFormat):
		return time.Parse(dateFormat, v)
	}

	return time.Time{}, errors.New("invalid format")
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
		&plog.LogRequest{
			LogKeys: plog.LogKeys{
				Severity: plog.Error,
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
