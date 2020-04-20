package main

import (
	"bufio"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kingpin"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/version"
)

var (
	pbmCmd = kingpin.New("pbm", "Percona Backup for MongoDB")
	mURL   = pbmCmd.Flag("mongodb-uri", "MongoDB connection string").String()

	configCmd           = pbmCmd.Command("config", "Set, change or list the config")
	configRsyncBcpListF = configCmd.Flag("force-resync", "Resync backup list with the current store").Bool()
	configListF         = configCmd.Flag("list", "List current settings").Bool()
	configFileF         = configCmd.Flag("file", "Upload config from YAML file").String()
	configSetF          = configCmd.Flag("set", "Set the option value <key.name=value>").StringMap()
	configShowKey       = configCmd.Arg("key", "Show the value of a specified key").String()

	backupCmd      = pbmCmd.Command("backup", "Make backup")
	bcpCompression = pbmCmd.Flag("compression", "Compression type <none>/<gzip>/<snappy>/<lz4>/<s2>/<pgzip>").
			Default(string(pbm.CompressionTypeGZIP)).
			Enum(string(pbm.CompressionTypeNone), string(pbm.CompressionTypeGZIP),
			string(pbm.CompressionTypeSNAPPY), string(pbm.CompressionTypeLZ4),
			string(pbm.CompressionTypeS2), string(pbm.CompressionTypePGZIP),
		)

	restoreCmd     = pbmCmd.Command("restore", "Restore backup")
	restoreBcpName = restoreCmd.Arg("backup_name", "Backup name to restore").Required().String()

	listCmd            = pbmCmd.Command("list", "Backup list")
	listCmdRestore     = listCmd.Flag("restore", "Show last N restores").Default("false").Bool()
	listCmdRestoreFull = listCmd.Flag("full", "Show extended restore info").Default("false").Short('f').Hidden().Bool()
	listCmdSize        = listCmd.Flag("size", "Show last N backups").Default("0").Int64()

	deleteBcpCmd    = pbmCmd.Command("delete-backup", "Delete a backup")
	deleteBcpName   = deleteBcpCmd.Arg("name", "backup name").String()
	deleteBcpCmdOtF = deleteBcpCmd.Flag("older-than", "Delete backups older than").String()
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
			for k, v := range *configSetF {
				err := pbmClient.SetConfigVar(k, v)
				if err != nil {
					log.Fatalln("Error: set config key:", err)
				}
				fmt.Printf("[%s=%s]\n", k, v)
			}
			rsync(pbmClient)
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
			err = pbmClient.SetConfigByte(buf)
			if err != nil {
				log.Fatalln("Error: unable to set config:", err)
			}

			fmt.Println("[Config set]\n------")
			getConfig(pbmClient)
			rsync(pbmClient)
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
	case restoreCmd.FullCommand():
		err := restore(pbmClient, *restoreBcpName)
		if err != nil {
			log.Fatalln("Error:", err)
		}
		fmt.Printf("Restore of the snapshot from '%s' has started\n", *restoreBcpName)
	case listCmd.FullCommand():
		if *listCmdRestore {
			printRestoreList(pbmClient, *listCmdSize, *listCmdRestoreFull)
		} else {
			printBackupList(pbmClient, *listCmdSize)
		}
	case deleteBcpCmd.FullCommand():
		if !*deleteBcpForceF {
			fmt.Print("Are you shure you want delete backup(s)? [y/N] ")
			scanner := bufio.NewScanner(os.Stdin)
			scanner.Scan()
			switch strings.TrimSpace(scanner.Text()) {
			case "yes", "Yes", "YES", "Y", "y":
			default:
				return
			}
		}

		var err error
		if len(*deleteBcpCmdOtF) > 0 {
			err = pbmClient.DeleteOlderThan(*deleteBcpCmdOtF)
		} else {
			if len(*deleteBcpName) == 0 {
				log.Fatalln("Error: backup name should be specified")
			}
			err = pbmClient.DeleteBackup(*deleteBcpName)
		}
		if err != nil {
			log.Fatalln("Error:", err)
		}
		printBackupList(pbmClient, 0)
	}
}

func rsync(pbmClient *pbm.PBM) {
	err := pbmClient.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdResyncBackupList,
	})
	if err != nil {
		log.Fatalln("Error: schedule resync:", err)
	}
	fmt.Printf("Backup list resync from the store has started\n")
}

func getConfig(pbmClient *pbm.PBM) {
	cfg, err := pbmClient.GetConfigYaml(true)
	if err != nil {
		log.Fatalln("Error: unable to get config:", err)
	}
	fmt.Println(string(cfg))
}
