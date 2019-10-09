package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/alecthomas/kingpin"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/version"
)

var (
	pbmCmd = kingpin.New("pbm", "Percona Backup for MongoDB")
	mURL   = pbmCmd.Flag("mongodb-uri", "MongoDB connection string").String()

	storageCmd     = pbmCmd.Command("store", "Target store")
	storageSetCmd  = storageCmd.Command("set", "Set store")
	storageConfig  = storageSetCmd.Flag("config", "Store config file in yaml format").String()
	storageShowCmd = storageCmd.Command("show", "Show current storage configuration")

	backupCmd      = pbmCmd.Command("backup", "Make backup")
	bcpCompression = pbmCmd.Flag("compression", "Compression type <none>/<gzip>").Hidden().
			Default(pbm.CompressionTypeGZIP).
			Enum(string(pbm.CompressionTypeNone), string(pbm.CompressionTypeGZIP))

	restoreCmd     = pbmCmd.Command("restore", "Restore backup")
	restoreBcpName = restoreCmd.Arg("backup_name", "Backup name to restore").Required().String()

	listCmd     = pbmCmd.Command("list", "Backup list")
	listCmdSize = listCmd.Flag("size", "Show last N backups").Default("0").Int64()

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
		log.Println("Error: parse command line parameters:", err)
		return
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
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pbmClient, err := pbm.New(ctx, *mURL)
	if err != nil {
		log.Println("Error: connect to mongodb:", err)
		return
	}

	switch cmd {
	case storageSetCmd.FullCommand():
		buf, err := ioutil.ReadFile(*storageConfig)
		if err != nil {
			log.Println("Error: unable to read storage file:", err)
			return
		}
		err = pbmClient.SetStorageByte(buf)
		if err != nil {
			log.Println("Error: unable to set storage:", err)
			return
		}
		fmt.Println("[Done]")
	case storageShowCmd.FullCommand():
		stg, err := pbmClient.GetStorageYaml(true)
		if err != nil {
			log.Println("Error: unable to get store:", err)
			return
		}
		fmt.Printf("Store\n-------\n%s\n", stg)
	case backupCmd.FullCommand():
		stg, err := pbmClient.GetStorage()
		if err != nil {
			if err == mongo.ErrNoDocuments {
				log.Println("Error: no store set. Set remote store with <pbm store set>.")
			} else {
				log.Println("Error: backup: get remote-store", err)
			}
			return
		}

		bcpName := time.Now().UTC().Format(time.RFC3339)
		if stg.Filesystem.Path != "" {
			bcpName = stg.Filesystem.Path + "/" + bcpName
		}
		err = pbmClient.SendCmd(pbm.Cmd{
			Cmd: pbm.CmdBackup,
			Backup: pbm.BackupCmd{
				Name:        bcpName,
				Compression: pbm.CompressionType(*bcpCompression),
			},
		})
		if err != nil {
			log.Println("Error: backup:", err)
			return
		}
		storeString := "s3://"
		if stg.S3.EndpointURL != "" {
			storeString += stg.S3.EndpointURL + "/"
		}
		storeString += stg.S3.Bucket
		if stg.Filesystem.Path != "" {
			storeString += "/" + stg.Filesystem.Path
		}
		fmt.Printf("Beginning backup '%s' to remote store %s\n", bcpName, storeString)
	case restoreCmd.FullCommand():
		err := pbmClient.SendCmd(pbm.Cmd{
			Cmd: pbm.CmdRestore,
			Restore: pbm.RestoreCmd{
				BackupName: *restoreBcpName,
			},
		})
		if err != nil {
			log.Println("Error: schedule restore:", err)
			return
		}
		fmt.Printf("Beginning restore of the snapshot from %s\n", *restoreBcpName)
	case listCmd.FullCommand():
		bcps, err := pbmClient.BackupsList(*listCmdSize)
		if err != nil {
			log.Println("Error: unable to get backups list:", err)
			return
		}
		fmt.Println("Backup history:")
		for _, b := range bcps {
			var bcp string
			switch b.Status {
			case pbm.StatusDone:
				bcp = b.Name
			case pbm.StatusRunnig:
				bcp = fmt.Sprintf("%s\tIn progress (Launched at %s)", b.Name, time.Unix(b.StartTS, 0).Format(time.RFC3339))
			case pbm.StatusError:
				bcp = fmt.Sprintf("%s\tFailed with \"%s\"", b.Name, b.Error)
			}

			fmt.Println(" ", bcp)
		}
	}
}
