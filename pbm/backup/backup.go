package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/mongodb/mongo-tools-common/db"
	mlog "github.com/mongodb/mongo-tools-common/log"
	"github.com/mongodb/mongo-tools-common/options"
	"github.com/mongodb/mongo-tools-common/progress"
	"github.com/mongodb/mongo-tools/mongodump"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/storage/s3"
	"github.com/percona/percona-backup-mongodb/version"
)

func init() {
	// set date format for mongo tools (mongodump/mongorestore) logger
	//
	// duplicated in backup/restore packages just
	// in the sake of clarity
	mlog.SetDateFormat(plog.LogTimeFormat)
}

type Backup struct {
	cn   *pbm.PBM
	node *pbm.Node
	ctx  context.Context
	opid string
}

func New(ctx context.Context, cn *pbm.PBM, node *pbm.Node) *Backup {
	return &Backup{
		cn:   cn,
		node: node,
		ctx:  ctx,
	}
}

// Run runs the backup
func (b *Backup) Run(bcp pbm.BackupCmd, opid pbm.OPID, l *plog.Event) (err error) {
	return b.run(bcp, opid, l)
}

type errMetaExists struct {
	error
}

func (e *errMetaExists) Error() string {
	return fmt.Sprintf("write backup meta to db: %v", e.error)
}

func (e *errMetaExists) Unwrap() error { return e.error }

// run the backup.
// TODO: describe flow
func (b *Backup) run(bcp pbm.BackupCmd, opid pbm.OPID, l *plog.Event) (err error) {
	inf, err := b.node.GetInfo()
	if err != nil {
		return errors.Wrap(err, "get cluster info")
	}

	b.opid = opid.String()
	meta := &pbm.BackupMeta{
		OPID:        b.opid,
		Name:        bcp.Name,
		StartTS:     time.Now().Unix(),
		Compression: bcp.Compression,
		Status:      pbm.StatusStarting,
		Replsets:    []pbm.BackupReplset{},
		LastWriteTS: primitive.Timestamp{T: 1, I: 1}, // (andrew) I dunno why, but the driver (mongo?) sets TS to the current wall clock if TS was 0, so have to init with 1
		PBMVersion:  version.DefaultInfo.Version,
	}

	rsName := inf.SetName

	rsMeta := pbm.BackupReplset{
		Name:         rsName,
		OplogName:    getDstName("oplog", bcp, inf.SetName),
		DumpName:     getDstName("dump", bcp, inf.SetName),
		StartTS:      time.Now().UTC().Unix(),
		Status:       pbm.StatusRunning,
		Conditions:   []pbm.Condition{},
		FirstWriteTS: primitive.Timestamp{T: 1, I: 1},
	}

	stg, err := b.cn.GetStorage(l)
	if err != nil {
		return errors.Wrap(err, "unable to get PBM storage configuration settings")
	}
	// on any error the RS' and the backup' (in case this is the backup leader) meta will be marked aproprietly
	defer func() {
		if err != nil {
			// protection from the lagging leader node.
			// if the leader node wasn't able to create meta (it's prjbably already exists)
			// it shouldn't try change anything, otherwise it may commit errors state to the
			// meta of already done backup. see https://jira.percona.com/browse/PBM-520
			if _, ok := err.(*errMetaExists); ok {
				return
			}

			status := pbm.StatusError
			if errors.Is(err, ErrCancelled) {
				status = pbm.StatusCancelled

				meta.Status = pbm.StatusCancelled
				meta.Replsets = append(meta.Replsets, rsMeta)
				l.Info("delete artefacts from storage: %v", b.cn.DeleteBackupFiles(meta, stg))
			}

			// protection from the lagging node.
			// during the StatusStarting stage a meta doesn't created yet,
			// so the node can mark with the error only meta of already finished backup
			// this is what we want to prevent. see https://jira.percona.com/browse/PBM-520
			if meta.Status != pbm.StatusStarting {
				ferr := b.cn.ChangeRSState(bcp.Name, rsMeta.Name, status, err.Error())
				l.Info("mark RS as %s `%v`: %v", status, err, ferr)
			}
			if inf.IsLeader() {
				ferr := b.cn.ChangeBackupState(bcp.Name, status, err.Error())
				l.Info("mark backup as %s `%v`: %v", status, err, ferr)
			}
		}
	}()

	ver, err := b.node.GetMongoVersion()
	if err == nil {
		meta.MongoVersion = ver.VersionString
	}

	cfg, err := b.cn.GetConfig()
	if err == pbm.ErrStorageUndefined {
		return errors.New("backups cannot be saved because PBM storage configuration hasn't been set yet")
	} else if err != nil {
		return errors.Wrap(err, "unable to get PBM config settings")
	}
	meta.Store = cfg.Storage
	// Erase credentials data
	meta.Store.S3.Credentials = s3.Credentials{}

	if inf.IsLeader() {
		err = b.cn.SetBackupMeta(meta)
		if err != nil {
			return &errMetaExists{err}
		}

		hbstop := make(chan struct{})
		defer close(hbstop)
		go func() {
			tk := time.NewTicker(time.Second * 5)
			defer tk.Stop()
			for {
				select {
				case <-tk.C:
					err := b.cn.BackupHB(bcp.Name)
					if err != nil {
						l.Error("send pbm heartbeat: %v", err)
					}
				case <-hbstop:
					return
				}
			}
		}()
	}

	// Waiting for StatusStarting to move further.
	// In case some preparations has to be done before backup.
	err = b.waitForStatus(bcp.Name, pbm.StatusStarting)
	if err != nil {
		return errors.Wrap(err, "waiting for start")
	}

	// A fallback. If there are some leftovers and main node
	// hasn't cleaned up it yet we should stop and notify user
	// since backup would be unrestorable.
	err = b.node.EnsureNoTMPcoll()
	if err != nil {
		return errors.Wrap(err, "EnsureNoTMPcoll")
	}

	rsMeta.Status = pbm.StatusRunning
	err = b.cn.AddRSMeta(bcp.Name, rsMeta)
	if err != nil {
		return errors.Wrap(err, "add shard's metadata")
	}

	if inf.IsLeader() {
		err := b.reconcileStatus(bcp.Name, pbm.StatusRunning, inf, &pbm.WaitBackupStart)
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

	err = b.cn.SetRSFirstWrite(bcp.Name, rsMeta.Name, oplogTS)
	if err != nil {
		return errors.Wrap(err, "set shard's first write ts")
	}

	sz, err := b.node.SizeDBs()
	if err != nil {
		return errors.Wrap(err, "mongodump")
	}
	// if backup wouldn't be compressed we're assuming
	// that the dump size could be up to 4x lagrer due to
	// mongo's wieredtiger compression
	if bcp.Compression == pbm.CompressionTypeNone {
		sz *= 4
	}

	dump := newDump(b.node.ConnURI(), b.node.DumpConns())
	_, err = Upload(b.ctx, dump, stg, bcp.Compression, rsMeta.DumpName, sz)
	if err != nil {
		return errors.Wrap(err, "mongodump")
	}
	l.Info("mongodump finished, waiting for the oplog")

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

	if inf.IsLeader() {
		err := b.reconcileStatus(bcp.Name, pbm.StatusDumpDone, inf, nil)
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

	oplog.SetTailingSpan(oplogTS, lwTS)
	// size -1 - we're assuming oplog never exceed 97Gb (see comments in s3.Save method)
	_, err = Upload(b.ctx, oplog, stg, bcp.Compression, rsMeta.OplogName, -1)
	if err != nil {
		return errors.Wrap(err, "oplog")
	}
	err = b.cn.ChangeRSState(bcp.Name, rsMeta.Name, pbm.StatusDone, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDone")
	}

	if inf.IsLeader() {
		err = b.reconcileStatus(bcp.Name, pbm.StatusDone, inf, nil)
		if err != nil {
			return errors.Wrap(err, "check cluster for backup done")
		}

		err = b.dumpClusterMeta(bcp.Name, stg)
		if err != nil {
			return errors.Wrap(err, "dump metadata")
		}
	}

	// to be sure the locks released only after the "done" status had written
	err = b.waitForStatus(bcp.Name, pbm.StatusDone)
	return errors.Wrap(err, "waiting for done")
}

const maxReplicationLagTimeSec = 21

// NodeSuits checks if node can perform backup
func NodeSuits(node *pbm.Node, inf *pbm.NodeInfo) (bool, error) {
	if inf.IsStandalone() {
		return false, errors.New("mongod node can not be used to fetch a consistent backup because it has no oplog. Please restart it as a primary in a single-node replicaset to make it compatible with PBM's backup method using the oplog")
	}

	// for the cases when no secondary was good enough for backup or there are no secondaries alive
	// wait for 90% of WaitBackupStart and then try to acquire a lock.
	// by that time healthy secondaries should have already acquired a lock.
	//
	// but no need to wait if this is the only node (the single-node replica set).
	//
	// TODO ? there is still a chance that the lock gonna be stolen from the healthy secondary node
	// TODO ? (due tmp network issues node got the command later than the primary, but it's maybe for the good that the node with the faulty network doesn't start the backup)
	if inf.IsPrimary && inf.Me == inf.Primary && len(inf.Hosts) > 1 {
		time.Sleep(pbm.WaitBackupStart * 9 / 10)
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

type Source interface {
	io.WriterTo
}

// ErrCancelled means backup was canceled
var ErrCancelled = errors.New("backup canceled")

// Upload writes data to dst from given src and returns an amount of written bytes
func Upload(ctx context.Context, src Source, dst storage.Storage, compression pbm.CompressionType, fname string, sizeb int) (int64, error) {
	r, pw := io.Pipe()

	w := Compress(pw, compression)

	var err rwErr
	var n int64
	go func() {
		n, err.read = src.WriteTo(w)
		err.compress = w.Close()
		pw.Close()
	}()

	saveDone := make(chan struct{})
	go func() {
		err.write = dst.Save(fname, r, sizeb)
		saveDone <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		err := r.Close()
		if err != nil {
			return 0, errors.Wrap(err, "cancel backup: close reader")
		}
		return 0, ErrCancelled
	case <-saveDone:
	}

	r.Close()

	if !err.nil() {
		return 0, err
	}

	return n, nil
}

func (b *Backup) reconcileStatus(bcpName string, status pbm.Status, ninf *pbm.NodeInfo, timeout *time.Duration) error {
	shards := []pbm.Shard{
		{
			ID:   ninf.SetName,
			Host: ninf.SetName + "/" + strings.Join(ninf.Hosts, ","),
		},
	}

	if ninf.IsSharded() {
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

// convergeClusterWithTimeout waits up to the geiven timeout until all given shards reached `status` and then updates the cluster status
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

	clusterTime, err := b.cn.ClusterTime()
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	for _, sh := range shards {
		for _, shard := range bmeta.Replsets {
			if shard.Name == sh.ID {
				// check if node alive
				lock, err := b.cn.GetLockData(&pbm.LockHeader{
					Type:    pbm.CmdBackup,
					OPID:    b.opid,
					Replset: shard.Name,
				})

				// nodes are cleaning its locks moving to the done status
				// so no lock is ok and no need to ckech the heartbeats
				if status != pbm.StatusDone && err != mongo.ErrNoDocuments {
					if err != nil {
						return false, errors.Wrapf(err, "unable to read lock for shard %s", shard.Name)
					}
					if lock.Heartbeat.T+pbm.StaleFrameSec < clusterTime.T {
						return false, errors.Errorf("lost shard %s, last beat ts: %d", shard.Name, lock.Heartbeat.T)
					}
				}

				// check status
				switch shard.Status {
				case status:
					shardsToFinish--
				case pbm.StatusCancelled:
					return false, ErrCancelled
				case pbm.StatusError:
					return false, errors.Errorf("backup on shard %s failed with: %s", shard.Name, bmeta.Error)
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

			clusterTime, err := b.cn.ClusterTime()
			if err != nil {
				return errors.Wrap(err, "read cluster time")
			}

			if bmeta.Hb.T+pbm.StaleFrameSec < clusterTime.T {
				return errors.Errorf("backup stuck, last beat ts: %d", bmeta.Hb.T)
			}

			switch bmeta.Status {
			case status:
				return nil
			case pbm.StatusCancelled:
				return ErrCancelled
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

func (b *Backup) dumpClusterMeta(bcpName string, stg storage.Storage) error {
	meta, err := b.cn.GetBackupMeta(bcpName)
	if err != nil {
		return errors.Wrap(err, "get backup metadata")
	}

	return writeMeta(stg, meta)
}

func writeMeta(stg storage.Storage, meta *pbm.BackupMeta) error {
	b, err := json.MarshalIndent(meta, "", "\t")
	if err != nil {
		return errors.Wrap(err, "marshal data")
	}

	err = stg.Save(meta.Name+pbm.MetadataFileSuffix, bytes.NewReader(b), -1)
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

type mdump struct {
	opts  *options.ToolOptions
	conns int
}

func newDump(curi string, conns int) *mdump {
	if conns <= 0 {
		conns = 1
	}
	return &mdump{
		opts: &options.ToolOptions{
			AppName:    "mongodump",
			VersionStr: "0.0.1",
			URI:        &options.URI{ConnectionString: curi},
			Auth:       &options.Auth{},
			Namespace:  &options.Namespace{},
			Connection: &options.Connection{},
			Direct:     true,
		},
		conns: conns,
	}
}

// "logger" for the mongodup's ProgressManager.
// need it to be able to write new progress data in a new line
type progressWriter struct{}

func (*progressWriter) Write(m []byte) (int, error) {
	log.Printf("%s", m)
	return len(m), nil
}

// Write always return 0 as written bytes. Needed to satisfy interface
func (d *mdump) WriteTo(w io.Writer) (int64, error) {
	pm := progress.NewBarWriter(&progressWriter{}, time.Second*60, 24, false)
	mdump := mongodump.MongoDump{
		ToolOptions: d.opts,
		OutputOptions: &mongodump.OutputOptions{
			// Archive = "-" means, for mongodump, use the provided Writer
			// instead of creating a file. This is not clear at plain sight,
			// you nee to look the code to discover it.
			Archive:                "-",
			NumParallelCollections: d.conns,
		},
		InputOptions:    &mongodump.InputOptions{},
		SessionProvider: &db.SessionProvider{},
		OutputWriter:    w,
		ProgressManager: pm,
	}
	err := mdump.Init()
	if err != nil {
		return 0, errors.Wrap(err, "init")
	}
	pm.Start()
	defer pm.Stop()

	err = mdump.Dump()

	return 0, errors.Wrap(err, "make dump")
}

func getDstName(typ string, bcp pbm.BackupCmd, rsName string) string {
	name := bcp.Name

	if rsName != "" {
		name += "_" + rsName
	}

	name += "." + typ

	switch bcp.Compression {
	case pbm.CompressionTypeGZIP, pbm.CompressionTypePGZIP:
		name += ".gz"
	case pbm.CompressionTypeLZ4:
		name += ".lz4"
	case pbm.CompressionTypeSNAPPY:
		name += ".snappy"
	case pbm.CompressionTypeS2:
		name += ".s2"
	}

	return name
}
