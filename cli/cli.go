package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
)

const (
	datetimeFormat = "2006-01-02T15:04:05"
	dateFormat     = "2006-01-02"
)

type outFormat string

const (
	outJSON outFormat = "json"
	outText outFormat = "text"
)

func Main() {
	var (
		pbmCmd  = kingpin.New("pbm", "Percona Backup for MongoDB")
		mURL    = pbmCmd.Flag("mongodb-uri", "MongoDB connection string (Default = PBM_MONGODB_URI environment variable)").String()
		pbmOutF = outFormat(*pbmCmd.Flag("out", "Output format <text>/<json>").Short('o').Default(string(outText)).Enum(string(outJSON), string(outText)))

		configCmd = pbmCmd.Command("config", "Set, change or list the config")
		cfg       = configOpts{
			rsync: *configCmd.Flag("force-resync", "Resync backup list with the current store").Bool(),
			list:  *configCmd.Flag("list", "List current settings").Bool(),
			file:  *configCmd.Flag("file", "Upload config from YAML file").String(),
			set:   *configCmd.Flag("set", "Set the option value <key.name=value>").StringMap(),
			key:   *configCmd.Arg("key", "Show the value of a specified key").String(),
		}

		backupCmd = pbmCmd.Command("backup", "Make backup")
		backup    = backupOpts{
			compression: pbm.CompressionType(*backupCmd.Flag("compression", "Compression type <none>/<gzip>/<snappy>/<lz4>/<s2>/<pgzip>").
				Default(string(pbm.CompressionTypeS2)).
				Enum(string(pbm.CompressionTypeNone), string(pbm.CompressionTypeGZIP),
					string(pbm.CompressionTypeSNAPPY), string(pbm.CompressionTypeLZ4),
					string(pbm.CompressionTypeS2), string(pbm.CompressionTypePGZIP),
				)),
		}

		cancelBcpCmd = pbmCmd.Command("cancel-backup", "Cancel backup")

		restoreCmd = pbmCmd.Command("restore", "Restore backup")
		restore    = restoreOpts{
			bcp:      *restoreCmd.Arg("backup_name", "Backup name to restore").String(),
			pitr:     *restoreCmd.Flag("time", fmt.Sprintf("Restore to the point-in-time. Set in format %s", datetimeFormat)).String(),
			pitrBase: *restoreCmd.Flag("base-snapshot", "Override setting: Name of older snapshot that PITR will be based on during restore.").String(),
		}

		listCmd = pbmCmd.Command("list", "Backup list")
		list    = listOpts{
			restore: *listCmd.Flag("restore", "Show last N restores").Default("false").Bool(),
			full:    *listCmd.Flag("full", "Show extended restore info").Default("false").Short('f').Hidden().Bool(),
			size:    *listCmd.Flag("size", "Show last N backups").Default("0").Int64(),
		}
	)

	if *mURL == "" {
		log.Println("Error: no mongodb connection URI supplied")
		log.Println("       Usual practice is the set it by the PBM_MONGODB_URI environment variable. It can also be set with commandline argument --mongodb-uri.")
		pbmCmd.Usage(os.Args[1:])
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pbmClient, err := pbm.New(ctx, *mURL, "pbm-ctl")
	if err != nil {
		log.Fatalln("Error: connect to mongodb:", err)
	}
	// TODO: need to decouple "logger" and "logs printer"
	pbmClient.InitLogger("", "")

	cmd, err := pbmCmd.DefaultEnvars().Parse(os.Args[1:])
	if err != nil {
		log.Fatalln("Error: parse command line parameters:", err)
	}

	var out fmt.Stringer
	switch cmd {
	case configCmd.FullCommand():
		out, err = runConfig(pbmClient, &cfg)
	case backupCmd.FullCommand():
		backup.name = time.Now().UTC().Format(time.RFC3339)
		if pbmOutF == outJSON {
			fmt.Printf("Starting backup '%s'\n", backup.name)
		}
		out, err = runBackup(pbmClient, &backup, pbmOutF)
	case cancelBcpCmd.FullCommand():
		out, err = cancelBcp(pbmClient)
	case restoreCmd.FullCommand():
		out, err = runRestore(pbmClient, &restore)
	case listCmd.FullCommand():
		out, err = runList(pbmClient, &list)
	}

	if err != nil {
		log.Fatalln("ERROR:", err)
	}

	switch pbmOutF {
	case outJSON:
		err := json.NewEncoder(os.Stdout).Encode(out)
		if err != nil {
			fmt.Fprintln(os.Stderr, "ERROR: encode output:", err)
		}
	default:
		fmt.Println(out)
	}
}

func runList(cn *pbm.PBM, l *listOpts) (fmt.Stringer, error) {
	if l.restore {
		return restoreList(cn, l.size, l.full)
	}
	// show message ans skip when resync is running
	lk, err := findLock(cn, cn.GetLocks)
	if err == nil && lk != nil && lk.Type == pbm.CmdResyncBackupList {
		return outMsg("Storage resync is running. Backups list will be available after sync finishes."), nil
	}

	return backupList(cn, l.size, l.full)
}

type backupListOut struct {
	Snapshots []snapshotStat `json:"snapshots"`
	PITR      struct {
		On     bool        `json:"on"`
		Ranges []pitrRange `json:"ranges"`
	} `json:"pitr"`
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
	Err   string `json:"error,omitempty"`
	Range struct {
		Start int64 `json:"start"`
		End   int64 `json:"end"`
	} `json:"range"`
	Size int64 `json:"size,omitempty"`
}

func (bl backupListOut) String() string {
	s := fmt.Sprintln("Backup snapshots:")
	for _, b := range bl.Snapshots {
		s += fmt.Sprintf("  %s [complete: %s]\n", b.Name, fmtTS(int64(b.StateTS)))
	}
	if bl.PITR.On {
		s += fmt.Sprintln("\nPITR <on>:")
	} else {
		s += fmt.Sprintln("\nPITR <off>:")
	}

	for _, r := range bl.PITR.Ranges {
		s += fmt.Sprintf("  %s - %s\n", fmtTS(r.Range.Start), fmtTS(r.Range.End))
	}

	return s
}

func backupList(cn *pbm.PBM, size int64, full bool) (list backupListOut, err error) {
	list.Snapshots, err = getSnapshotList(cn, size)
	if err != nil {
		return nil, err
	}
}

func fmtTS(ts int64) string {
	return time.Unix(ts, 0).UTC().Format("2006-01-02T15:04:05")
}

type outMsg string

func (m outMsg) String() (s string) {
	return string(m)
}

func cancelBcp(cn *pbm.PBM) (fmt.Stringer, error) {
	err := cn.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdCancelBackup,
	})
	if err != nil {
		return nil, errors.Wrap(err, "send backup canceling")
	}
	return outMsg("Backup cancellation has started"), nil
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
		// It could happen that the healthy `lk` became stale by the time this check
		// or the op was finished and the new one was started. So the `l.Type != lk.Type`
		// would be true but for the legit reason (no error).
		// But chances for that are quite low and on the next run of `pbm status` everythin
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
