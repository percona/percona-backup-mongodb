package backup

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/options"
	"github.com/mongodb/mongo-tools-common/progress"
	"github.com/mongodb/mongo-tools/mongodump"
	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func Backup(name string, cn *pbm.PBM, node *pbm.Node) error {
	ctx, stopOplog := context.WithCancel(context.Background())
	defer stopOplog()

	stg, err := cn.GetStorage()
	if err != nil {
		return errors.Wrap(err, "unable to get backup storage")
	}
	if stg.Type == pbm.StorageUndef {
		return errors.Wrap(err, "storage doesn't set, you have to set storage to make backup")
	}

	oplogDst, err := Destination(stg, name+".oplog", pbm.CompressionTypeNo)
	if err != nil {
		return errors.Wrap(err, "storage writer")
	}

	ot := OplogTail(cn, node)
	err = ot.Run(ctx)
	if err != nil {
		return errors.Wrap(err, "run oplog tailer")
	}

	// writing an oplog into the target storage
	go func() {
		_, err = io.Copy(oplogDst, ot)
		oplogDst.Close()
	}()

	// TODO we have to wait until the oplog wrinting process beign startd
	time.Sleep(1)
	if err != nil {
		errors.Wrap(err, "io copy from the oplog reader to the storage writer")
	}

	bcpDst, err := Destination(stg, name+".dump", pbm.CompressionTypeNo)
	if err != nil {
		return errors.Wrap(err, "storage writer")
	}
	defer bcpDst.Close()

	return errors.Wrap(mdump(bcpDst, node.ConnURI()), "mongodump")
}

func mdump(to io.WriteCloser, curi string) error {
	opts := options.ToolOptions{
		AppName:    "mongodump",
		VersionStr: "0.0.1",
		URI:        &options.URI{ConnectionString: curi},
		Auth:       &options.Auth{},
		Namespace:  &options.Namespace{},
		Connection: &options.Connection{},
	}

	d := mongodump.MongoDump{
		ToolOptions: &opts,
		OutputOptions: &mongodump.OutputOptions{
			// Archive = "-" means, for mongodump, use the provided Writer
			// instead of creating a file. This is not clear at plain sight,
			// you nee to look the code to discover it.
			Archive:                "-",
			NumParallelCollections: 1,
		},
		InputOptions:    &mongodump.InputOptions{},
		SessionProvider: &db.SessionProvider{},
		OutputWriter:    to,
		ProgressManager: progress.NewBarWriter(os.Stdout, time.Second*3, 24, false),
	}
	err := d.Init()
	if err != nil {
		return errors.Wrap(err, "init")
	}
	return errors.Wrap(d.Dump(), "dump")
}
