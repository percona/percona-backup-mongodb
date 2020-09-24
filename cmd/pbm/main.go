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
		err = pbmClient.SendCmd(cmd)
		if err != nil {
			log.Fatalln("Error: schedule delete:", err)
		}
		fmt.Println("Backup deletion from the store has started")
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
