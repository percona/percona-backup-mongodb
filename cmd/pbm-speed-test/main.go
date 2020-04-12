package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/percona/percona-backup-mongodb/speedt"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/alecthomas/kingpin"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/version"
)

func main() {
	var (
		tCmd = kingpin.New("pbm-speed-test", "Percona Backup for MongoDB compression and upload speed test")
		mURL = tCmd.Flag("mongodb-uri", "MongoDB connection string").Envar("PBM_MONGODB_URI").Required().String()

		compressionCmd = tCmd.Command("compression", "Run compression test")
		compressType   = compressionCmd.Flag("compression", "Compression type <none>/<gzip>/<snappy>/<lz4>/<s2>/<pgzip>").
				Default(string(pbm.CompressionTypeGZIP)).
				Enum(string(pbm.CompressionTypeNone), string(pbm.CompressionTypeGZIP),
				string(pbm.CompressionTypeSNAPPY), string(pbm.CompressionTypeLZ4),
				string(pbm.CompressionTypeS2), string(pbm.CompressionTypePGZIP),
			)
		compressSampleCol = compressionCmd.Flag("sample-collection", "Set collection as the data source").Short('c').String()

		storageCmd  = tCmd.Command("storage", "Run storage test")
		storageSize = storageCmd.Flag("size-gb", "Set data size in GB. Default 1").Short('s').Float64()

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

	if *storageSize == 0 {
		*storageSize = 1
	}

	ctx := context.Background()
	node, err := pbm.NewNode(ctx, "node", *mURL)
	if err != nil {
		log.Fatalln("Error: connect to mongodb-node:", err)
	}
	defer node.Session().Disconnect(ctx)

	switch cmd {
	case compressionCmd.FullCommand():
		fmt.Println("Test started")
		compression(node.Session(), pbm.CompressionType(*compressType), *storageSize, *compressSampleCol)
	case storageCmd.FullCommand():
		fmt.Println("Test started")
		storage(node, pbm.CompressionType(*compressType), *storageSize, *compressSampleCol)
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

func compression(cn *mongo.Client, compression pbm.CompressionType, sizeGb float64, collection string) {
	stg := pbm.Storage{
		Type: pbm.StorageBlackHole,
	}

	r, err := speedt.Run(cn, stg, compression, sizeGb, collection)
	if err != nil {
		log.Fatalln("Error:", err)
	}

	fmt.Println(r)
}

func storage(node *pbm.Node, compression pbm.CompressionType, sizeGb float64, collection string) {
	ctx := context.Background()

	pbmClient, err := pbm.New(ctx, node.ConnURI(), "pbm-speed-test")
	if err != nil {
		log.Fatalln("Error: connect to mongodb-pbm:", err)
	}
	defer pbmClient.Conn.Disconnect(ctx)

	stg, err := pbmClient.GetStorage()
	if err != nil {
		log.Fatalln("Error: get storage:", err)
	}

	r, err := speedt.Run(node.Session(), stg, compression, sizeGb, collection)
	if err != nil {
		log.Fatalln("Error:", err)
	}

	fmt.Println(r)
}
