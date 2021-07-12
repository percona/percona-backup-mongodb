package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
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

type deleteBcpOpts struct {
	name      string
	olderThan string
	force     bool
}

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
			size:    *listCmd.Flag("size", "Show last N backups").Default("0").Int(),
		}

		deleteBcpCmd = pbmCmd.Command("delete-backup", "Delete a backup")
		deleteBcp    = deleteBcpOpts{
			name:      *deleteBcpCmd.Arg("name", "Backup name").String(),
			olderThan: *deleteBcpCmd.Flag("older-than", fmt.Sprintf("Delete backups older than date/time in format %s or %s", datetimeFormat, dateFormat)).String(),
			force:     *deleteBcpCmd.Flag("force", "Force. Don't ask confirmation").Short('f').Bool(),
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
	case deleteBcpCmd.FullCommand():
		out, err = deleteBackup(pbmClient, &deleteBcp, pbmOutF)
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

func deleteBackup(pbmClient *pbm.PBM, d *deleteBcpOpts, outf outFormat) (fmt.Stringer, error) {
	if !d.force && isTTY() {
		fmt.Print("Are you sure you want delete backup(s)? [y/N] ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		switch strings.TrimSpace(scanner.Text()) {
		case "yes", "Yes", "YES", "Y", "y":
		default:
			return nil, nil
		}
	}

	cmd := pbm.Cmd{
		Cmd: pbm.CmdDeleteBackup,
	}
	if len(d.olderThan) > 0 {
		t, err := parseDateT(d.olderThan)
		if err != nil {
			return nil, errors.Wrap(err, "parse date")
		}
		cmd.Delete.OlderThan = t.UTC().Unix()
	} else {
		if len(d.name) == 0 {
			return nil, errors.New("backup name should be specified")
		}
		cmd.Delete.Backup = d.name
	}
	tsop := time.Now().UTC().Unix()
	err := pbmClient.SendCmd(cmd)
	if err != nil {
		return nil, errors.Wrap(err, "schedule delete")
	}
	if outf != outText {
		return nil, nil
	}

	fmt.Print("Waiting for delete to be done ")
	err = waitOp(pbmClient,
		&pbm.LockHeader{
			Type: pbm.CmdDeleteBackup,
		},
		time.Second*60)
	if err != nil && err != errTout {
		return nil, err
	}

	errl, err := lastLogErr(pbmClient, pbm.CmdDeleteBackup, tsop)
	if err != nil {
		return nil, errors.Wrap(err, "read agents log")
	}

	if errl != "" {
		return nil, errors.New(errl)
	}

	if err == errTout {
		fmt.Println("\nOperation is still in progress, please check status in a while")
	} else {
		time.Sleep(time.Second)
		fmt.Print(".")
		time.Sleep(time.Second)
		fmt.Println("[done]")
	}

	return runList(pbmClient, &listOpts{})
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
	if len(l) == 0 {
		return "", nil
	}

	if l[0].TS < after {
		return "", nil
	}

	return l[0].Msg, nil
}
