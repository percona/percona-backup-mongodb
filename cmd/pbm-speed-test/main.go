package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/alecthomas/kingpin"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/storage/blackhole"
	"github.com/percona/percona-backup-mongodb/internal/util"
	"github.com/percona/percona-backup-mongodb/internal/version"
	"github.com/percona/percona-backup-mongodb/pbm"
)

func main() {
	var (
		tCmd        = kingpin.New("pbm-speed-test", "Percona Backup for MongoDB compression and upload speed test")
		mURL        = tCmd.Flag("mongodb-uri", "MongoDB connection string").Envar("PBM_MONGODB_URI").String()
		sampleColF  = tCmd.Flag("sample-collection", "Set collection as the data source").Short('c').String()
		sampleSizeF = tCmd.Flag("size-gb", "Set data size in GB. Default 1").Short('s').Float64()

		compressLevelArg []int
		compressLevel    *int

		compressType = tCmd.Flag("compression", "Compression type <none>/<gzip>/<snappy>/<lz4>/<s2>/<pgzip>/<zstd>").
				Default(string(defs.CompressionTypeS2)).
				Enum(string(defs.CompressionTypeNone), string(defs.CompressionTypeGZIP),
				string(defs.CompressionTypeSNAPPY), string(defs.CompressionTypeLZ4),
				string(defs.CompressionTypeS2), string(defs.CompressionTypePGZIP),
				string(defs.CompressionTypeZstandard),
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

	rand.Seed(time.Now().UnixNano())

	switch cmd {
	case compressionCmd.FullCommand():
		fmt.Print("Test started ")
		testCompression(*mURL, defs.CompressionType(*compressType), compressLevel, *sampleSizeF, *sampleColF)
	case storageCmd.FullCommand():
		fmt.Print("Test started ")
		testStorage(*mURL, defs.CompressionType(*compressType), compressLevel, *sampleSizeF, *sampleColF)
	case versionCmd.FullCommand():
		switch {
		case *versionCommit:
			fmt.Println(version.Current().GitCommit)
		case *versionShort:
			fmt.Println(version.Current().Short())
		default:
			fmt.Println(version.Current().All(*versionFormat))
		}
	}
}

func testCompression(mURL string, compression defs.CompressionType, level *int, sizeGb float64, collection string) {
	ctx := context.Background()

	var cn *mongo.Client

	if collection != "" {
		node, err := pbm.NewNode(ctx, mURL, 1)
		if err != nil {
			log.Fatalln("Error: connect to mongodb-node:", err)
		}
		cn = node.Session()

		defer cn.Disconnect(ctx) //nolint:errcheck
	}

	stg := blackhole.New()
	done := make(chan struct{})
	go printw(done)

	r, err := doTest(cn, stg, compression, level, sizeGb, collection)
	if err != nil {
		log.Fatalln("Error:", err)
	}

	done <- struct{}{}
	fmt.Println()
	fmt.Println(r)
}

func testStorage(mURL string, compression defs.CompressionType, level *int, sizeGb float64, collection string) {
	ctx := context.Background()

	node, err := pbm.NewNode(ctx, mURL, 1)
	if err != nil {
		log.Fatalln("Error: connect to mongodb-node:", err)
	}
	sess := node.Session()

	defer sess.Disconnect(ctx) //nolint:errcheck

	pbmClient, err := pbm.New(ctx, mURL, "pbm-speed-test")
	if err != nil {
		log.Fatalln("Error: connect to mongodb-pbm:", err)
	}
	defer pbmClient.Conn.Disconnect(ctx) //nolint:errcheck

	l := pbmClient.Logger().NewEvent("", "", "", primitive.Timestamp{})
	stg, err := util.GetStorage(ctx, pbmClient.Conn, l)
	if err != nil {
		log.Fatalln("Error: get storage:", err)
	}
	done := make(chan struct{})
	go printw(done)
	r, err := doTest(sess, stg, compression, level, sizeGb, collection)
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
