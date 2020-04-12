package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/alecthomas/kingpin"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/version"
)

func main() {
	var (
		tCmd = kingpin.New("pbm-speed-test", "Percona Backup for MongoDB compression and upload speed test")
		mURL = tCmd.Flag("mongodb-uri", "MongoDB connection string").String()

		compressionCmd = tCmd.Command("compression", "Run compression test")
		compressType   = compressionCmd.Flag("type", "Compression type <none>/<gzip>").
				Default(pbm.CompressionTypeGZIP).
				Enum(string(pbm.CompressionTypeNone), string(pbm.CompressionTypeGZIP))
		compressSampleCol = compressionCmd.Flag("sample-collection", "Upload config from YAML file").Short('c').String()

		storageCmd  = tCmd.Command("storage", "Run storage test")
		storageSize = storageCmd.Flag("size-gb", "Upload config from YAML file").Short('s').Default("1").Int()

		versionCmd    = tCmd.Command("version", "PBM version info")
		versionShort  = versionCmd.Flag("short", "Only version info").Default("false").Bool()
		versionCommit = versionCmd.Flag("commit", "Only git commit info").Default("false").Bool()
		versionFormat = versionCmd.Flag("format", "Output format <json or \"\">").Default("").String()
	)

	cmd, err := tCmd.DefaultEnvars().Parse(os.Args[1:])
	if err != nil && cmd != versionCmd.FullCommand() {
		log.Println("Error: Parse command line parameters:", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pbmClient, err := pbm.New(ctx, *mURL, "pbm-speed-test")
	if err != nil {
		log.Fatalln("Error: connect to mongodb-pbm:", err)
	}
	defer pbmClient.Conn.Disconnect(ctx)
	node, err := pbm.NewNode(ctx, "node", *mURL)
	if err != nil {
		log.Fatalln("Error: connect to mongodb-node:", err)
	}
	defer node.Session().Disconnect(ctx)

	switch cmd {
	case compressionCmd.FullCommand():
	case storageCmd.FullCommand():
	case versionCmd.FullCommand():
		switch {
		case *versionCommit:
			fmt.Println(version.DefaultInfo.GitCommit)
		case *versionShort:
			fmt.Println(version.DefaultInfo.Short())
		default:
			fmt.Println(version.DefaultInfo.All(*versionFormat))
		}
	}
}
