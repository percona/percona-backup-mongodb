package backup

import (
	"context"
	"encoding/json"
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

// Run runs the backup
func Run(bcp pbm.BackupCmd, cn *pbm.PBM, node *pbm.Node) (err error) {
	ctx, stopOplog := context.WithCancel(context.Background())
	defer stopOplog()

	meta := &pbm.BackupMeta{
		Name:        bcp.Name,
		StartTS:     time.Now().UTC().Unix(),
		Compression: bcp.Compression,
		Status:      pbm.StatusRunnig,
	}

	ver, err := node.GetMongoVersion()
	if err == nil {
		meta.MongoVersion = ver.VersionString
	}

	defer func() {
		if err != nil {
			meta.Status = pbm.StatusError
			meta.Error = err.Error()
		}
		cn.UpdateBackupMeta(meta)
	}()

	stg, err := cn.GetStorage()
	if err != nil {
		return errors.Wrap(err, "unable to get backup store")
	}
	if stg.Type == pbm.StorageUndef {
		return errors.Wrap(err, "store doesn't set, you have to set store to make backup")
	}
	meta.Store = stg
	// Erase credentials data
	meta.Store.S3.Credentials = pbm.Credentials{}

	im, err := node.GetIsMaster()
	if err != nil {
		return errors.Wrap(err, "get cluster info")
	}
	meta.RsName = im.SetName
	meta.OplogName = getDstName("oplog", bcp, im.SetName)
	meta.DumpName = getDstName("dump", bcp, im.SetName)

	err = cn.UpdateBackupMeta(meta)
	if err != nil {
		return errors.Wrap(err, "write backup meta to db")
	}

	oplogDst, oplogCloser, err := Destination(stg, meta.OplogName, bcp.Compression)
	if err != nil {
		return errors.Wrap(err, "oplog store writer")
	}

	ot := OplogTail(cn, node)
	err = ot.Run(ctx)
	if err != nil {
		return errors.Wrap(err, "run oplog tailer")
	}

	// writing an oplog to the target store
	go func() {
		_, err = io.Copy(oplogDst, ot)
		oplogDst.Close()
		if oplogCloser != nil {
			oplogCloser.Close()
		}
	}()

	// TODO we have to wait until the oplog wrinting process being startd. Not sleep.
	time.Sleep(1)
	if err != nil {
		errors.Wrap(err, "io copy from the oplog reader to the store writer")
	}

	bcpDst, bcpCloser, err := Destination(stg, meta.DumpName, bcp.Compression)
	if err != nil {
		return errors.Wrap(err, "dump store writer")
	}
	defer func() {
		bcpDst.Close()
		if bcpCloser != nil {
			bcpCloser.Close()
		}
	}()

	err = mdump(bcpDst, node.ConnURI())
	if err != nil {
		return errors.Wrap(err, "mongodump")
	}

	mw, _, err := Destination(stg, bcp.Name+"_"+im.SetName+".pbm.json", pbm.CompressionTypeNone)
	if err != nil {
		return errors.Wrap(err, "create writer for metadata")
	}
	defer mw.Close()

	menc := json.NewEncoder(mw)
	menc.SetIndent("", "\t")

	meta.Status = pbm.StatusDone
	meta.EndTS = time.Now().UTC().Unix()
	err = menc.Encode(meta)
	if err != nil {
		return errors.Wrap(err, "write to store")
	}

	return nil
}

// NodeQualify checks if node qualify to perform backup
func NodeQualify(bcp pbm.BackupCmd, node *pbm.Node) (bool, error) {
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

func getDstName(typ string, bcp pbm.BackupCmd, rsName string) string {
	name := bcp.Name

	if rsName != "" {
		name += "_" + rsName
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
