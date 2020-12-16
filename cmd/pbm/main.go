package main

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/yaml.v2"

	"github.com/percona/percona-backup-mongodb/pbm"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/version"
)

var (
	pbmCmd = kingpin.New("pbm", "Percona Backup for MongoDB")
	mURL   = pbmCmd.Flag("mongodb-uri", "MongoDB connection string (Default = PBM_MONGODB_URI environment variable)").String()

	configCmd           = pbmCmd.Command("config", "Set, change or list the config")
	configRsyncBcpListF = configCmd.Flag("force-resync", "Resync backup list with the current store").Bool()
	configListF         = configCmd.Flag("list", "List current settings").Bool()
	configFileF         = configCmd.Flag("file", "Upload config from YAML file").String()
	configSetF          = configCmd.Flag("set", "Set the option value <key.name=value>").StringMap()
	configShowKey       = configCmd.Arg("key", "Show the value of a specified key").String()

	backupCmd      = pbmCmd.Command("backup", "Make backup")
	bcpCompression = pbmCmd.Flag("compression", "Compression type <none>/<gzip>/<snappy>/<lz4>/<s2>/<pgzip>").
			Default(string(pbm.CompressionTypeS2)).
			Enum(string(pbm.CompressionTypeNone), string(pbm.CompressionTypeGZIP),
			string(pbm.CompressionTypeSNAPPY), string(pbm.CompressionTypeLZ4),
			string(pbm.CompressionTypeS2), string(pbm.CompressionTypePGZIP),
		)

	restoreCmd     = pbmCmd.Command("restore", "Restore backup")
	restoreBcpName = restoreCmd.Arg("backup_name", "Backup name to restore").String()
	restorePITRF   = restoreCmd.Flag("time", fmt.Sprintf("Restore to the point-in-time. Set in format %s", datetimeFormat)).String()

	cancelBcpCmd = pbmCmd.Command("cancel-backup", "Restore backup")

	listCmd        = pbmCmd.Command("list", "Backup list")
	listCmdRestore = listCmd.Flag("restore", "Show last N restores").Default("false").Bool()
	listCmdFullF   = listCmd.Flag("full", "Show extended restore info").Default("false").Short('f').Hidden().Bool()
	listCmdSize    = listCmd.Flag("size", "Show last N backups").Default("0").Int64()

	deleteBcpCmd    = pbmCmd.Command("delete-backup", "Delete a backup")
	deleteBcpName   = deleteBcpCmd.Arg("name", "Backup name").String()
	deleteBcpCmdOtF = deleteBcpCmd.Flag("older-than", fmt.Sprintf("Delete backups older than date/time in format %s or %s", datetimeFormat, dateFormat)).String()
	deleteBcpForceF = deleteBcpCmd.Flag("force", "Force. Don't ask confirmation").Short('f').Bool()

	versionCmd    = pbmCmd.Command("version", "PBM version info")
	versionShort  = versionCmd.Flag("short", "Only version info").Default("false").Bool()
	versionCommit = versionCmd.Flag("commit", "Only git commit info").Default("false").Bool()
	versionFormat = versionCmd.Flag("format", "Output format <json or \"\">").Default("").String()

	logsCmd    = pbmCmd.Command("logs", "PBM logs")
	logsTailF  = logsCmd.Flag("tail", "Show last N entries").Short('t').Default("20").Int64()
	logsNodeF  = logsCmd.Flag("node", "Target node in format replset[/host:posrt]").Short('n').String()
	logsTypeF  = logsCmd.Flag("severity", "Severity level D, I, W, E or F, low to high. Choosing one includes higher levels too.").Short('s').Default("I").Enum("D", "I", "W", "E", "F")
	logsEventF = logsCmd.Flag("event", "Event in format backup[/2020-10-06T11:45:14Z]").Short('e').String()
	logsOPIDF  = logsCmd.Flag("opid", "Operation ID").Short('i').String()
	logsOutF   = logsCmd.Flag("out", "Event in format backup[/2020-10-06T11:45:14Z]").Short('o').Default("text").Enum("json", "text")

	client *mongo.Client
)

func main() {
	log.SetFlags(0)

	cmd, err := pbmCmd.DefaultEnvars().Parse(os.Args[1:])
	if err != nil && cmd != versionCmd.FullCommand() {
		log.Fatalln("Error: parse command line parameters:", err)
	}

	if cmd == versionCmd.FullCommand() {
		switch {
		case *versionCommit:
			fmt.Println(version.DefaultInfo.GitCommit)
		case *versionShort:
			fmt.Println(version.DefaultInfo.Short())
		default:
			fmt.Println(version.DefaultInfo.All(*versionFormat))
		}
		return
	}

	if *mURL == "" {
		log.Println("Error: no mongodb connection URI supplied\n")
		log.Println("       Usual practice is the set it by the PBM_MONGODB_URI environment variable. It can also be set with commandline argument --mongodb-uri.\n")
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

	switch cmd {
	case configCmd.FullCommand():
		switch {
		case len(*configSetF) > 0:
			rsnc := false
			for k, v := range *configSetF {
				err := pbmClient.SetConfigVar(k, v)
				if err != nil {
					log.Fatalln("Error: set config key:", err)
				}
				fmt.Printf("[%s=%s]\n", k, v)

				path := strings.Split(k, ".")
				if !rsnc && len(path) > 0 && path[0] == "storage" {
					rsnc = true
				}
			}
			if rsnc {
				rsync(pbmClient)
			}
		case len(*configShowKey) > 0:
			k, err := pbmClient.GetConfigVar(*configShowKey)
			if err != nil {
				log.Fatalln("Error: unable to get config key:", err)
			}
			fmt.Println(k)
		case *configRsyncBcpListF:
			rsync(pbmClient)
		case len(*configFileF) > 0:
			buf, err := ioutil.ReadFile(*configFileF)
			if err != nil {
				log.Fatalln("Error: unable to read config file:", err)
			}

			var cfg pbm.Config
			err = yaml.UnmarshalStrict(buf, &cfg)
			if err != nil {
				log.Fatalln("Error: unable to  unmarshal config file:", err)
			}

			cCfg, err := pbmClient.GetConfig()
			if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
				log.Fatalln("Error: unable to get current config:", err)
			}

			err = pbmClient.SetConfigByte(buf)
			if err != nil {
				log.Fatalln("Error: unable to set config:", err)
			}

			fmt.Println("[Config set]\n------")
			getConfig(pbmClient)

			// provider value may differ as it set automatically after config parsing
			cCfg.Storage.S3.Provider = cfg.Storage.S3.Provider
			// resync storage only if Storage options have changed
			if !reflect.DeepEqual(cfg.Storage, cCfg.Storage) {
				rsync(pbmClient)
			}
		case *configListF:
			fallthrough
		default:
			getConfig(pbmClient)
		}
	case backupCmd.FullCommand():
		bcpName := time.Now().UTC().Format(time.RFC3339)
		fmt.Printf("Starting backup '%s'", bcpName)
		storeString, err := backup(pbmClient, bcpName, *bcpCompression)
		if err != nil {
			log.Fatalln("\nError starting backup:", err)
			return
		}
		fmt.Printf("\nBackup '%s' to remote store '%s' has started\n", bcpName, storeString)
	case cancelBcpCmd.FullCommand():
		err := pbmClient.SendCmd(pbm.Cmd{
			Cmd: pbm.CmdCancelBackup,
		})
		if err != nil {
			log.Fatalln("Error: send backup canceling:", err)
		}
		fmt.Printf("Backup cancellation has started\n")
	case restoreCmd.FullCommand():
		if *restorePITRF != "" && *restoreBcpName != "" {
			log.Fatalln("Error: either a backup name or point in time should be set, non both together!")
		}
		switch {
		case *restoreBcpName != "":
			err := restore(pbmClient, *restoreBcpName)
			if err != nil {
				log.Fatalln("Error:", err)
			}
			fmt.Printf("Restore of the snapshot from '%s' has started\n", *restoreBcpName)
		case *restorePITRF != "":
			err := pitrestore(pbmClient, *restorePITRF)
			if err != nil {
				log.Fatalln("Error:", err)
			}
			fmt.Printf("Restore to the point in time '%s' has started\n", *restorePITRF)
		default:
			log.Fatalln("Error: undefined restore state")
		}
	case listCmd.FullCommand():
		if *listCmdRestore {
			printRestoreList(pbmClient, *listCmdSize, *listCmdFullF)
		} else {
			printBackupList(pbmClient, *listCmdSize)
			printPITR(pbmClient, int(*listCmdSize), *listCmdFullF)
		}
	case deleteBcpCmd.FullCommand():
		deleteBackup(pbmClient)
	case logsCmd.FullCommand():
		logs(pbmClient)
	}
}

func isTTY() bool {
	fi, err := os.Stdin.Stat()
	return (fi.Mode()&os.ModeCharDevice) != 0 && err == nil
}

func rsync(pbmClient *pbm.PBM) {
	err := pbmClient.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdResyncBackupList,
	})
	if err != nil {
		log.Fatalln("Error: schedule resync:", err)
	}
	fmt.Println("Backup list resync from the store has started")
}

func getConfig(pbmClient *pbm.PBM) {
	cfg, err := pbmClient.GetConfigYaml(true)
	if err != nil {
		log.Fatalln("Error: unable to get config:", err)
	}
	fmt.Println(string(cfg))
}

func deleteBackup(pbmClient *pbm.PBM) {
	if !*deleteBcpForceF && isTTY() {
		// we don't care about the error since all this is only to show an additional notice
		pitrOn, _ := pbmClient.IsPITR()
		if pitrOn {
			fmt.Println("While PITR in ON the last snapshot won't be deleted")
			fmt.Println()
		}

		fmt.Print("Are you sure you want delete backup(s)? [y/N] ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		switch strings.TrimSpace(scanner.Text()) {
		case "yes", "Yes", "YES", "Y", "y":
		default:
			return
		}
	}

	cmd := pbm.Cmd{
		Cmd: pbm.CmdDeleteBackup,
	}
	if len(*deleteBcpCmdOtF) > 0 {
		t, err := parseDateT(*deleteBcpCmdOtF)
		if err != nil {
			log.Fatalln("Error: parse date:", err)
		}
		cmd.Delete.OlderThan = t.UTC().Unix()
	} else {
		if len(*deleteBcpName) == 0 {
			log.Fatalln("Error: backup name should be specified")
		}
		cmd.Delete.Backup = *deleteBcpName
	}
	tsop := time.Now().UTC().Unix()
	err := pbmClient.SendCmd(cmd)
	if err != nil {
		log.Fatalln("Error: schedule delete:", err)
	}

	fmt.Print("Waiting for delete to be done ")
	err = waitOp(pbmClient,
		&pbm.LockHeader{
			Type: pbm.CmdDeleteBackup,
		},
		time.Second*60)
	if err != nil && err != errTout {
		log.Fatalln("\nError:", err)
	}

	errl, err := lastLogErr(pbmClient, pbm.CmdDeleteBackup, tsop)
	if err != nil {
		log.Fatalln("\nError: read agents log:", err)
	}

	if errl != "" {
		log.Fatalln("\nError:", errl)
	}

	if err == errTout {
		fmt.Println("\nOperation is still in progress, please check agents' logs in a while")
	} else {
		fmt.Println("[done]")
	}
	printBackupList(pbmClient, 0)
	printPITR(pbmClient, 0, false)
}

const (
	datetimeFormat = "2006-01-02T15:04:05"
	dateFormat     = "2006-01-02"
)

func parseDateT(v string) (time.Time, error) {
	switch len(v) {
	case len(datetimeFormat):
		return time.Parse(datetimeFormat, v)
	case len(dateFormat):
		return time.Parse(dateFormat, v)
	}

	return time.Time{}, errors.New("invalid format")
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
