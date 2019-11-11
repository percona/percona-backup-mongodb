package backup

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/options"
	"github.com/mongodb/mongo-tools-common/progress"
	"github.com/mongodb/mongo-tools/mongodump"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm"
)

type Backup struct {
	cn   *pbm.PBM
	node *pbm.Node
	name string
}

func New(cn *pbm.PBM, node *pbm.Node) *Backup {
	return &Backup{
		cn:   cn,
		node: node,
	}
}

// Run runs the backup
func (b *Backup) Run(bcp pbm.BackupCmd) (err error) {
	return b.run(bcp)
}

var statusRunTimeout = time.Second * 15

// run the backup.
// TODO: describe flow
func (b *Backup) run(bcp pbm.BackupCmd) (err error) {
	im, err := b.node.GetIsMaster()
	if err != nil {
		return errors.Wrap(err, "get cluster info")
	}

	meta := &pbm.BackupMeta{
		Name:        bcp.Name,
		StartTS:     time.Now().UTC().Unix(),
		Compression: bcp.Compression,
		Status:      pbm.StatusStarting,
		Replsets:    []pbm.BackupReplset{},
		LastWriteTS: primitive.Timestamp{T: 1, I: 1}, // (andrew) I dunno why, but the driver (mongo?) sets TS to the current wall clock if TS was 0, so have to init with 1
	}

	rsName := im.SetName
	if rsName == "" {
		rsName = pbm.NoReplset
	}
	rsMeta := pbm.BackupReplset{
		Name:       rsName,
		OplogName:  getDstName("oplog", bcp, im.SetName),
		DumpName:   getDstName("dump", bcp, im.SetName),
		StartTS:    time.Now().UTC().Unix(),
		Status:     pbm.StatusRunning,
		Conditions: []pbm.Condition{},
	}

	defer func() {
		if err != nil {
			b.cn.ChangeBackupState(bcp.Name, pbm.StatusError, err.Error())
			b.cn.ChangeRSState(bcp.Name, rsMeta.Name, pbm.StatusError, err.Error())
		}
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

	if im.IsLeader() {
		err = b.cn.SetBackupMeta(meta)
		if err != nil {
			return errors.Wrap(err, "write backup meta to db")
		}
	}

	// Waiting for StatusStarting to move further.
	// In case some preparations has to be done before backup.
	err = b.waitForStatus(bcp.Name, pbm.StatusStarting)
	if err != nil {
		return errors.Wrap(err, "waiting for start")
	}

	rsMeta.Status = pbm.StatusRunning
	err = b.cn.AddRSMeta(bcp.Name, rsMeta)
	if err != nil {
		return errors.Wrap(err, "add shard's metadata")
	}

	if im.IsLeader() {
		err := b.reconcileStatus(bcp.Name, pbm.StatusRunning, im, &statusRunTimeout)
		if err != nil {
			if errors.Cause(err) == errConvergeTimeOut {
				return errors.Wrap(err, "couldn't get response from all shards")
			}
			return errors.Wrap(err, "check cluster for backup started")
		}
	}

	// Waiting for cluster's StatusRunning to move further.
	err = b.waitForStatus(bcp.Name, pbm.StatusRunning)
	if err != nil {
		return errors.Wrap(err, "waiting for start")
	}

	oplog := NewOplog(b.node)
	oplogTS, err := oplog.LastWrite()
	if err != nil {
		return errors.Wrap(err, "define oplog start position")
	}

	err = b.dump(stg, rsMeta.DumpName, bcp.Compression)
	if err != nil {
		return errors.Wrap(err, "mongodump")
	}
	log.Println("mongodump finished, waiting for the oplog")

	err = b.cn.ChangeRSState(bcp.Name, rsMeta.Name, pbm.StatusDumpDone, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDumpDone")
	}

	lwts, err := oplog.LastWrite()
	if err != nil {
		return errors.Wrap(err, "get shard's last write ts")
	}

	err = b.cn.SetRSLastWrite(bcp.Name, rsMeta.Name, lwts)
	if err != nil {
		return errors.Wrap(err, "set shard's last write ts")
	}

	if im.IsLeader() {
		err := b.reconcileStatus(bcp.Name, pbm.StatusDumpDone, im, nil)
		if err != nil {
			return errors.Wrap(err, "check cluster for dump done")
		}

		err = b.setClusterLastWrite(bcp.Name)
		if err != nil {
			return errors.Wrap(err, "set cluster last write ts")
		}
	}

	err = b.waitForStatus(bcp.Name, pbm.StatusDumpDone)
	if err != nil {
		return errors.Wrap(err, "waiting for dump done")
	}

	lwTS, err := b.waitForLastWrite(bcp.Name)
	if err != nil {
		return errors.Wrap(err, "waiting and reading cluster last write ts")
	}

	err = b.oplog(oplog, oplogTS, lwTS, stg, rsMeta.OplogName, bcp.Compression)
	if err != nil {
		return errors.Wrap(err, "oplog")
	}
	err = b.cn.ChangeRSState(bcp.Name, rsMeta.Name, pbm.StatusDone, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDone")
	}

	if im.IsLeader() {
		err = b.reconcileStatus(bcp.Name, pbm.StatusDone, im, nil)
		if err != nil {
			return errors.Wrap(err, "check cluster for backup done")
		}

		err = b.dumpClusterMeta(bcp.Name, stg)
		if err != nil {
			return errors.Wrap(err, "dump metadata")
		}
	}

	return nil
}

const maxReplicationLagTimeSec = 21

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

	replLag, err := node.ReplicationLag()
	if err != nil {
		return false, errors.Wrap(err, "get node replication lag")
	}

	return replLag < maxReplicationLagTimeSec && status.Health == pbm.NodeHealthUp &&
			(status.State == pbm.NodeStatePrimary || status.State == pbm.NodeStateSecondary),
		nil
}

// rwErr multierror for the read/compress/write-to-store operations set
type rwErr struct {
	read     error
	compress error
	write    error
}

func (rwe rwErr) Error() string {
	var r string
	if rwe.read != nil {
		r += "read data: " + rwe.read.Error() + "."
	}
	if rwe.compress != nil {
		r += "compress data: " + rwe.compress.Error() + "."
	}
	if rwe.write != nil {
		r += "write data: " + rwe.write.Error() + "."
	}

	return r
}
func (rwe rwErr) nil() bool {
	return rwe.read == nil && rwe.compress == nil && rwe.write == nil
}

func (b *Backup) oplog(oplog *Oplog, startTS, endTS primitive.Timestamp, stg pbm.Storage, name string, compression pbm.CompressionType) error {
	r, pw := io.Pipe()
	defer r.Close()

	w := Compress(pw, compression)

	var err rwErr
	go func() {
		err.read = oplog.SliceTo(b.cn.Context(), w, startTS, endTS)
		err.compress = w.Close()
		pw.Close()
	}()

	err.write = Save(r, stg, name)

	if !err.nil() {
		return err
	}

	return nil
}

func (b *Backup) reconcileStatus(bcpName string, status pbm.Status, im *pbm.IsMaster, timeout *time.Duration) error {
	shards := []pbm.Shard{
		{
			ID:   im.SetName,
			Host: im.SetName + "/" + strings.Join(im.Hosts, ","),
		},
	}

	if im.IsSharded() {
		s, err := b.cn.GetShards()
		if err != nil {
			return errors.Wrap(err, "get shards list")
		}
		shards = append(shards, s...)
	}

	if timeout != nil {
		return errors.Wrap(b.convergeClusterWithTimeout(bcpName, shards, status, *timeout), "convergeClusterWithTimeout")
	}
	return errors.Wrap(b.convergeCluster(bcpName, shards, status), "convergeCluster")
}

// convergeCluster waits until all given shards reached `status` and updates a cluster status
func (b *Backup) convergeCluster(bcpName string, shards []pbm.Shard, status pbm.Status) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			ok, err := b.converged(bcpName, shards, status)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		case <-b.cn.Context().Done():
			return nil
		}
	}
}

var errConvergeTimeOut = errors.New("reached converge timeout")

// convergeClusterWithTimeout waits up to geiven timeout until all given shards reached `status` and updates a cluster status
func (b *Backup) convergeClusterWithTimeout(bcpName string, shards []pbm.Shard, status pbm.Status, t time.Duration) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	tout := time.NewTicker(t)
	defer tout.Stop()
	for {
		select {
		case <-tk.C:
			ok, err := b.converged(bcpName, shards, status)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		case <-tout.C:
			return errConvergeTimeOut
		case <-b.cn.Context().Done():
			return nil
		}
	}
}

func (b *Backup) converged(bcpName string, shards []pbm.Shard, status pbm.Status) (bool, error) {
	shardsToFinish := len(shards)
	bmeta, err := b.cn.GetBackupMeta(bcpName)
	if err != nil {
		return false, errors.Wrap(err, "get backup metadata")
	}
	for _, sh := range shards {
		for _, shard := range bmeta.Replsets {
			if shard.Name == sh.ID {
				switch shard.Status {
				case status:
					shardsToFinish--
				case pbm.StatusError:
					bmeta.Status = pbm.StatusError
					bmeta.Error = shard.Error
					return false, errors.Wrapf(err, "backup on the shard %s failed with", shard.Name)
				}
			}
		}
	}
	if shardsToFinish == 0 {
		err := b.cn.ChangeBackupState(bcpName, status, "")
		if err != nil {
			return false, errors.Wrapf(err, "update backup meta with %s", status)
		}
		return true, nil
	}

	return false, nil
}

func (b *Backup) waitForStatus(bcpName string, status pbm.Status) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			bmeta, err := b.cn.GetBackupMeta(bcpName)
			if err != nil {
				return errors.Wrap(err, "get backup metadata")
			}
			switch bmeta.Status {
			case status:
				return nil
			case pbm.StatusError:
				return errors.Wrap(err, "backup failed")
			}
		case <-b.cn.Context().Done():
			return nil
		}
	}
}

func (b *Backup) waitForLastWrite(bcpName string) (primitive.Timestamp, error) {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			bmeta, err := b.cn.GetBackupMeta(bcpName)
			if err != nil {
				return primitive.Timestamp{}, errors.Wrap(err, "get backup metadata")
			}
			if bmeta.LastWriteTS.T > 0 {
				return bmeta.LastWriteTS, nil
			}
		case <-b.cn.Context().Done():
			return primitive.Timestamp{}, nil
		}
	}
}

func (b *Backup) dumpClusterMeta(bcpName string, stg pbm.Storage) error {
	meta, err := b.cn.GetBackupMeta(bcpName)
	if err != nil {
		return errors.Wrap(err, "get backup metadata")
	}

	return writeMeta(stg, meta)
}

func writeMeta(stg pbm.Storage, meta *pbm.BackupMeta) error {
	b, err := json.MarshalIndent(meta, "", "\t")
	if err != nil {
		return errors.Wrap(err, "marshal data")
	}

	err = Save(bytes.NewReader(b), stg, meta.Name+".pbm.json")
	return errors.Wrap(err, "write to store")
}

func (b *Backup) setClusterLastWrite(bcpName string) error {
	bmeta, err := b.cn.GetBackupMeta(bcpName)
	if err != nil {
		return errors.Wrap(err, "get backup metadata")
	}

	lw := primitive.Timestamp{}
	for _, rs := range bmeta.Replsets {
		if primitive.CompareTimestamp(lw, rs.LastWriteTS) == -1 {
			lw = rs.LastWriteTS
		}
	}

	err = b.cn.SetLastWrite(bcpName, lw)
	return errors.Wrap(err, "set timestamp")
}

func (b *Backup) dump(stg pbm.Storage, name string, compression pbm.CompressionType) error {
	r, pw := io.Pipe()
	w := Compress(pw, compression)

	var err rwErr
	go func() {
		err.read = mdump(w, b.node.ConnURI())
		err.compress = w.Close()
		pw.Close()
	}()

	err.write = Save(r, stg, name)

	if !err.nil() {
		return err
	}

	return nil
}

func mdump(to io.Writer, curi string) error {
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
