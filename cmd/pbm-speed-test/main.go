package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/percona/percona-backup-mongodb/speedt"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/storage/blackhole"
	"github.com/percona/percona-backup-mongodb/version"
)

func main() {
	var (
		tCmd        = kingpin.New("pbm-speed-test", "Percona Backup for MongoDB compression and upload speed test")
		mURL        = tCmd.Flag("mongodb-uri", "MongoDB connection string").Envar("PBM_MONGODB_URI").String()
		sampleColF  = tCmd.Flag("sample-collection", "Set collection as the data source").Short('c').String()
		sampleSizeF = tCmd.Flag("size-gb", "Set data size in GB. Default 1").Short('s').Float64()

		compressLevelArg []int
		compressLevel    *int

		compressType = tCmd.Flag("compression", "Compression type <none>/<gzip>/<snappy>/<lz4>/<s2>/<pgzip>").
				Default(string(pbm.CompressionTypeS2)).
				Enum(string(pbm.CompressionTypeNone), string(pbm.CompressionTypeGZIP),
				string(pbm.CompressionTypeSNAPPY), string(pbm.CompressionTypeLZ4),
				string(pbm.CompressionTypeS2), string(pbm.CompressionTypePGZIP),
			)

		compressionCmd = tCmd.Command("compression", "Run compression test")
		storageCmd     = tCmd.Command("storage", "Run storage test")

		versionCmd    = tCmd.Command("version", "PBM version info")
		versionShort  = versionCmd.Flag("short", "Only version info").Default("false").Bool()
		versionCommit = versionCmd.Flag("commit", "Only git commit info").Default("false").Bool()
		versionFormat = versionCmd.Flag("format", "Output format <json or \"\">").Default("").String()
	)

	tCmd.Flag("compression-level", "Compression level (specific to the compression type)").IntsVar(&compressLevelArg)

	if len(compressLevelArg) > 0 {
		compressLevel = &compressLevelArg[0]
	}

	cmd, err := tCmd.DefaultEnvars().Parse(os.Args[1:])
	if err != nil && cmd != versionCmd.FullCommand() {
		log.Println("Error: Parse command line parameters:", err)
		return
	}

	if *sampleSizeF == 0 {
		*sampleSizeF = 1
	}

	switch cmd {
	case compressionCmd.FullCommand():
		fmt.Print("Test started ")
		compression(*mURL, pbm.CompressionType(*compressType), compressLevel, *sampleSizeF, *sampleColF)
	case storageCmd.FullCommand():
		fmt.Print("Test started ")
		storage(*mURL, pbm.CompressionType(*compressType), compressLevel, *sampleSizeF, *sampleColF)
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

func compression(mURL string, compression pbm.CompressionType, level *int, sizeGb float64, collection string) {
	ctx := context.Background()

	var cn *mongo.Client

	if collection != "" {
		node, err := pbm.NewNode(ctx, mURL, 1)
		if err != nil {
			log.Fatalln("Error: connect to mongodb-node:", err)
		}
		defer node.Session().Disconnect(ctx)

		cn = node.Session()
	}

	stg := blackhole.New()
	done := make(chan struct{})
	go printw(done)

	r, err := speedt.Run(cn, stg, compression, level, sizeGb, collection)
	if err != nil {
		log.Fatalln("Error:", err)
	}

	done <- struct{}{}
	fmt.Println()
	fmt.Println(r)
}

func storage(mURL string, compression pbm.CompressionType, level *int, sizeGb float64, collection string) {
	ctx := context.Background()

	node, err := pbm.NewNode(ctx, mURL, 1)
	if err != nil {
		log.Fatalln("Error: connect to mongodb-node:", err)
	}
	defer node.Session().Disconnect(ctx)

	pbmClient, err := pbm.New(ctx, mURL, "pbm-speed-test")
	if err != nil {
		log.Fatalln("Error: connect to mongodb-pbm:", err)
	}
	defer pbmClient.Conn.Disconnect(ctx)

	stg, err := pbmClient.GetStorage(nil)
	if err != nil {
		log.Fatalln("Error: get storage:", err)
	}
	done := make(chan struct{})
	go printw(done)
	r, err := speedt.Run(node.Session(), stg, compression, level, sizeGb, collection)
	if err != nil {
		log.Fatalln("Error:", err)
	}

	done <- struct{}{}
	fmt.Println()
	fmt.Println(r)
}

func printw(done <-chan struct{}) {
	tk := time.NewTicker(time.Second * 2)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			fmt.Print(".")
		case <-done:
			return
		}
	}
}
