package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/alecthomas/kingpin"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm"
)

var (
	pbmCmd = kingpin.New("pbm", "Percona Backup for MongoDB")
	mURL   = pbmCmd.Flag("mongodb-uri", "MongoDB connection string").Required().String()

	storageCmd     = pbmCmd.Command("store", "Target store")
	storageSetCmd  = storageCmd.Command("set", "Set store")
	storageConfig  = storageSetCmd.Flag("config", "Store config file in yaml format").String()
	storageShowCmd = storageCmd.Command("show", "Show current storage configuration")

	backupCmd      = pbmCmd.Command("backup", "Make backup")
	bcpCompression = pbmCmd.Flag("compression", "Compression type <none>/<gzip>").
			Default(pbm.CompressionTypeGZIP).Enum(string(pbm.CompressionTypeNone), string(pbm.CompressionTypeGZIP))

	restoreCmd     = pbmCmd.Command("restore", "Restore backup")
	restoreBcpName = restoreCmd.Arg("backup_name", "Backup name to restore").Required().String()

	client *mongo.Client
)

func main() {
	cmd, err := pbmCmd.DefaultEnvars().Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "[ERROR] Parse command line parameters:", err)
	}

	*mURL = "mongodb://" + strings.Replace(*mURL, "mongodb://", "", 1)
	client, err = mongo.NewClient(options.Client().ApplyURI(*mURL))
	if err != nil {
		fmt.Fprintf(os.Stderr, "new mongo client: %v", err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mongo connect: %v", err)
		return
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mongo ping: %v", err)
		return
	}

	switch cmd {
	case storageSetCmd.FullCommand():
		buf, err := ioutil.ReadFile(*storageConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Unable to read storage file: %v", err)
			return
		}
		err = pbm.New(ctx, client, *mURL).SetStorageByte(buf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Unable to set storage: %v", err)
			return
		}
		fmt.Println("[Done]")
	case storageShowCmd.FullCommand():
		stg, err := pbm.New(ctx, client, *mURL).GetStorageYaml(true)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Unable to get storage: %v", err)
			return
		}
		fmt.Printf("Storage\n-------\n%s\n", stg)
	case backupCmd.FullCommand():
		bcpName := time.Now().UTC().Format(time.RFC3339)
		err := pbm.New(ctx, client, *mURL).SendCmd(pbm.Cmd{
			Cmd: pbm.CmdBackup,
			Backup: pbm.BackupCmd{
				Name:        bcpName,
				Compression: pbm.CompressionType(*bcpCompression),
			},
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Schedule backup: %v\n", err)
			return
		}
		fmt.Printf("Backup '%s' is scheduled", bcpName)
	case restoreCmd.FullCommand():
		err := pbm.New(ctx, client, *mURL).SendCmd(pbm.Cmd{
			Cmd: pbm.CmdRestore,
			Restore: pbm.RestoreCmd{
				BackupName: *restoreBcpName,
			},
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "[ERROR] Schedule restore: %v\n", err)
			return
		}
		fmt.Printf("Beginning restore of the snapshot from %s", *restoreBcpName)
	}
}
