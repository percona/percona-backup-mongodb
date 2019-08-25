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

// NodeQualify checks if node qualify to perform backup
func NodeQualify(bcp pbm.Backup, node *pbm.Node) (bool, error) {
	im, err := node.GetIsMaster()
	if err != nil {
		return false, errors.Wrap(err, "get isMaster data for node")
	}

	if im.IsMaster && !bcp.FromMaster {
		return false, nil
	}

	status, err := node.Status()
	if err != nil {
		return false, errors.Wrap(err, "get node status")
	}

	return status.Health == pbm.NodeHealthUp &&
			(status.State == pbm.NodeStatePrimary || status.State == pbm.NodeStateSecondary),
		nil
}

func Backup(bcp pbm.Backup, cn *pbm.PBM, node *pbm.Node) error {
	ctx, stopOplog := context.WithCancel(context.Background())
	defer stopOplog()

	stg, err := cn.GetStorage()
	if err != nil {
		return errors.Wrap(err, "unable to get backup store")
	}
	if stg.Type == pbm.StorageUndef {
		return errors.Wrap(err, "store doesn't set, you have to set store to make backup")
	}

	oplogName := getDstName("oplog", bcp, node)
	oplogDst, oplogCloser, err := Destination(stg, oplogName, bcp.Compression)
	if err != nil {
		return errors.Wrap(err, "oplog store writer")
	}

	ot := OplogTail(cn, node)
	err = ot.Run(ctx)
	if err != nil {
		return errors.Wrap(err, "run oplog tailer")
	}

	// writing an oplog into the target store
	go func() {
		_, err = io.Copy(oplogDst, ot)
		oplogDst.Close()
		if oplogCloser != nil {
			oplogCloser.Close()
		}
	}()

	// TODO we have to wait until the oplog wrinting process beign startd
	time.Sleep(1)
	if err != nil {
		errors.Wrap(err, "io copy from the oplog reader to the store writer")
	}

	bcpName := getDstName("dump", bcp, node)
	bcpDst, bcpCloser, err := Destination(stg, bcpName, bcp.Compression)
	if err != nil {
		return errors.Wrap(err, "dump store writer")
	}
	defer func() {
		bcpDst.Close()
		if bcpCloser != nil {
			bcpCloser.Close()
		}
	}()

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
	return errors.Wrap(d.Dump(), "make dump")
}

func getDstName(typ string, bcp pbm.Backup, node *pbm.Node) string {
	name := bcp.Name

	im, err := node.GetIsMaster()
	if err == nil && im.SetName != "" {
		name += "_" + im.SetName
	}

	name += "." + typ

	switch bcp.Compression {
	case pbm.CompressionTypeGZIP:
		name += ".gz"
	case pbm.CompressionTypeLZ4:
		name += ".lz4"
	case pbm.CompressionTypeSNAPPY:
		name += ".snappy"
	}

	return name
}
