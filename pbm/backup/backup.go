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

type Backup struct {
	cn   *pbm.PBM
	node *pbm.Node
}

func New(cn *pbm.PBM, node *pbm.Node) *Backup {
	return &Backup{
		cn:   cn,
		node: node,
	}
}

// Run runs the backup
func (b *Backup) Run(bcp pbm.BackupCmd) (err error) {
	meta := &pbm.BackupMeta{
		Name:        bcp.Name,
		StartTS:     time.Now().UTC().Unix(),
		Compression: bcp.Compression,
		Status:      pbm.StatusRunnig,
		Replsets:    make(map[string]pbm.BackupReplset),
	}

	im, err := b.node.GetIsMaster()
	if err != nil {
		return errors.Wrap(err, "get cluster info")
	}

	rsName := im.SetName
	if rsName == "" {
		rsName = pbm.NoReplset
	}
	rsMeta := pbm.BackupReplset{
		Name:      rsName,
		OplogName: getDstName("oplog", bcp, im.SetName),
		DumpName:  getDstName("dump", bcp, im.SetName),
		StartTS:   time.Now().UTC().Unix(),
		Status:    pbm.StatusRunnig,
	}

	defer func() {
		if err != nil {
			rsMeta.Status = pbm.StatusError
			rsMeta.Error = err.Error()
		}

		meta.Replsets[rsName] = rsMeta
		b.cn.UpdateBackupMeta(meta)
	}()

	ver, err := b.node.GetMongoVersion()
	if err == nil {
		meta.MongoVersion = ver.VersionString
	}

	stg, err := b.cn.GetStorage()
	if err != nil {
		return errors.Wrap(err, "unable to get backup store")
	}
	if stg.Type == pbm.StorageUndef {
		return errors.Wrap(err, "store doesn't set, you have to set store to make backup")
	}
	meta.Store = stg
	// Erase credentials data
	meta.Store.S3.Credentials = pbm.Credentials{}

	err = b.cn.UpdateBackupMeta(meta)
	if err != nil {
		return errors.Wrap(err, "write backup meta to db")
	}

	stopOplog, err := b.oplog(stg, rsMeta.OplogName, bcp.Compression)
	if err != nil {
		return errors.Wrap(err, "oplog tailer")
	}
	defer stopOplog()

	err = b.dump(stg, rsMeta.DumpName, bcp.Compression)
	if err != nil {
		return errors.Wrap(err, "data dump")
	}

	rsMeta.Status = pbm.StatusDumpDone
	rsMeta.DumpDoneTS = time.Now().UTC().Unix()
	meta.Replsets[rsName] = rsMeta
	b.cn.UpdateBackupMeta(meta)

	if !im.IsSharded() {
		meta.Status = pbm.StatusDone
		meta.DoneTS = time.Now().UTC().Unix()
		return errors.Wrap(writeMeta(stg, meta), "save metadata to the store")
	}

	return nil
}

// Oplog tailing
func (b *Backup) oplog(stg pbm.Storage, name string, compression pbm.CompressionType) (context.CancelFunc, error) {
	oplogDst, oplogCloser, err := Destination(stg, name, compression)
	if err != nil {
		return nil, errors.Wrap(err, "oplog store writer")
	}

	ot := OplogTail(b.cn, b.node)

	ctx, stopOplog := context.WithCancel(context.Background())
	err = ot.Run(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "run oplog tailer")
	}

	// writing an oplog to the target store
	// io.Copy is going to run until Oplog tailer being closed
	go func() {
		io.Copy(oplogDst, ot)
		oplogDst.Close()
		if oplogCloser != nil {
			oplogCloser.Close()
		}
	}()

	// TODO we have to wait until the oplog wrinting process being started. Not sleep.
	time.Sleep(1)
	return stopOplog, nil
}

func writeMeta(stg pbm.Storage, meta *pbm.BackupMeta) error {
	mw, _, err := Destination(stg, meta.Name+".pbm.json", pbm.CompressionTypeNone)
	if err != nil {
		return errors.Wrap(err, "create writer")
	}
	defer mw.Close()

	menc := json.NewEncoder(mw)
	menc.SetIndent("", "\t")

	return errors.Wrap(menc.Encode(meta), "write to store")
}

// NodeSuits checks if node can perform backup
func NodeSuits(bcp pbm.BackupCmd, node *pbm.Node) (bool, error) {
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

func (b *Backup) dump(stg pbm.Storage, name string, compression pbm.CompressionType) error {
	bcpDst, bcpCloser, err := Destination(stg, name, compression)
	if err != nil {
		return errors.Wrap(err, "store writer")
	}
	defer func() {
		bcpDst.Close()
		if bcpCloser != nil {
			bcpCloser.Close()
		}
	}()

	return errors.Wrap(mdump(bcpDst, b.node.ConnURI()), "mongodump")
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
