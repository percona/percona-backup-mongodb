package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"time"

	mlog "github.com/mongodb/mongo-tools/common/log"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
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
	typ  pbm.BackupType
}

func New(cn *pbm.PBM, node *pbm.Node) *Backup {
	return &Backup{
		cn:   cn,
		node: node,
		typ:  pbm.LogicalBackup,
	}
}

func NewPhysical(cn *pbm.PBM, node *pbm.Node) *Backup {
	return &Backup{
		cn:   cn,
		node: node,
		typ:  pbm.PhysicalBackup,
	}
}

func (b *Backup) Init(bcp pbm.BackupCmd, opid pbm.OPID, balancer pbm.BalancerMode) error {
	ts, err := b.cn.ClusterTime()
	if err != nil {
		return errors.Wrap(err, "read cluster time")
	}

	meta := &pbm.BackupMeta{
		Type:           b.typ,
		OPID:           opid.String(),
		Name:           bcp.Name,
		Compression:    bcp.Compression,
		StartTS:        time.Now().Unix(),
		Status:         pbm.StatusStarting,
		Replsets:       []pbm.BackupReplset{},
		LastWriteTS:    primitive.Timestamp{T: 1, I: 1}, // the driver (mongo?) sets TS to the current wall clock if TS was 0, so have to init with 1
		FirstWriteTS:   primitive.Timestamp{T: 1, I: 1}, // the driver (mongo?) sets TS to the current wall clock if TS was 0, so have to init with 1
		PBMVersion:     version.DefaultInfo.Version,
		Nomination:     []pbm.BackupRsNomination{},
		BalancerStatus: balancer,
		Hb:             ts,
	}

	cfg, err := b.cn.GetConfig()
	if err == pbm.ErrStorageUndefined {
		return errors.New("backups cannot be saved because PBM storage configuration hasn't been set yet")
	} else if err != nil {
		return errors.Wrap(err, "unable to get PBM config settings")
	}
	meta.Store = cfg.Storage

	ver, err := b.node.GetMongoVersion()
	if err == nil {
		meta.MongoVersion = ver.VersionString
	}

	return b.cn.SetBackupMeta(meta)
}

// Run runs backup.
// TODO: describe flow
func (b *Backup) Run(ctx context.Context, bcp pbm.BackupCmd, opid pbm.OPID, l *plog.Event) (err error) {
	inf, err := b.node.GetInfo()
	if err != nil {
		return errors.Wrap(err, "get cluster info")
	}

	rsMeta := pbm.BackupReplset{
		Name:         inf.SetName,
		StartTS:      time.Now().UTC().Unix(),
		Status:       pbm.StatusRunning,
		Conditions:   []pbm.Condition{},
		FirstWriteTS: primitive.Timestamp{T: 1, I: 1},
	}

	stg, err := b.cn.GetStorage(l)
	if err != nil {
		return errors.Wrap(err, "unable to get PBM storage configuration settings")
	}

	bcpm, err := b.cn.GetBackupMeta(bcp.Name)
	if err != nil {
		return errors.Wrap(err, "balancer status, get backup meta")
	}

	// on any error the RS' and the backup' (in case this is the backup leader) meta will be marked appropriately
	defer func() {
		if err != nil {
			status := pbm.StatusError
			if errors.Is(err, ErrCancelled) {
				status = pbm.StatusCancelled

				meta := &pbm.BackupMeta{
					Name:     bcp.Name,
					Status:   pbm.StatusCancelled,
					Replsets: []pbm.BackupReplset{rsMeta},
				}

				l.Info("delete artifacts from storage: %v", b.cn.DeleteBackupFiles(meta, stg))
			}

			ferr := b.cn.ChangeRSState(bcp.Name, rsMeta.Name, status, err.Error())
			l.Info("mark RS as %s `%v`: %v", status, err, ferr)

			if inf.IsLeader() {
				ferr := b.cn.ChangeBackupState(bcp.Name, status, err.Error())
				l.Info("mark backup as %s `%v`: %v", status, err, ferr)
			}
		}

		// Turn the balancer back on if needed
		//
		// Every agent will check if the balancer was on before the backup started.
		// And will try to turn it on again if so. So if the leader node went down after turning off
		// the balancer some other node will bring it back.
		// TODO: what if all agents went down.
		if bcpm.BalancerStatus != pbm.BalancerModeOn {
			return
		}

		errd := b.cn.SetBalancerStatus(pbm.BalancerModeOn)
		if errd != nil {
			l.Error("set balancer ON: %v", errd)
			return
		}
		l.Debug("set balancer on")
	}()

	if inf.IsLeader() {
		hbstop := make(chan struct{})
		defer close(hbstop)
		err := b.cn.BackupHB(bcp.Name)
		if err != nil {
			return errors.Wrap(err, "init heartbeat")
		}
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

		if bcpm.BalancerStatus == pbm.BalancerModeOn {
			err = b.cn.SetBalancerStatus(pbm.BalancerModeOff)
			if err != nil {
				return errors.Wrap(err, "set balancer OFF")
			}
			l.Debug("waiting for balancer off")
			bs := waitForBalancerOff(b.cn, time.Second*30, l)
			l.Debug("balancer status: %s", bs)
		}
	}

	// Waiting for StatusStarting to move further.
	// In case some preparations has to be done before backup.
	err = b.waitForStatus(bcp.Name, pbm.StatusStarting, &pbm.WaitBackupStart)
	if err != nil {
		return errors.Wrap(err, "waiting for start")
	}

	switch b.typ {
	case pbm.LogicalBackup:
		err = b.doLogical(ctx, bcp, opid, &rsMeta, inf, stg, l)
	case pbm.PhysicalBackup:
		err = b.doPhysical(ctx, bcp, opid, &rsMeta, inf, stg, l)
	default:
		return errors.New("undefined backup type")
	}
	if err != nil {
		return err
	}

	err = b.cn.ChangeRSState(bcp.Name, rsMeta.Name, pbm.StatusDone, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDone")
	}

	if inf.IsLeader() {
		epch, err := b.cn.ResetEpoch()
		if err != nil {
			l.Error("reset epoch")
		} else {
			l.Debug("epoch set to %v", epch)
		}

		err = b.reconcileStatus(bcp.Name, opid.String(), pbm.StatusDone, nil)
		if err != nil {
			return errors.Wrap(err, "check cluster for backup done")
		}

		err = b.dumpClusterMeta(bcp.Name, stg)
		if err != nil {
			return errors.Wrap(err, "dump metadata")
		}
	}

	// to be sure the locks released only after the "done" status had written
	err = b.waitForStatus(bcp.Name, pbm.StatusDone, nil)
	return errors.Wrap(err, "waiting for done")
}

func waitForBalancerOff(cn *pbm.PBM, t time.Duration, l *plog.Event) pbm.BalancerMode {
	dn := time.NewTimer(t)
	defer dn.Stop()
	tk := time.NewTicker(time.Millisecond * 500)
	defer tk.Stop()

	var bs *pbm.BalancerStatus
	var err error
Loop:
	for {
		select {
		case <-tk.C:
			bs, err = cn.GetBalancerStatus()
			if err != nil {
				l.Error("get balancer status: %v", err)
				continue
			}
			if bs.Mode == pbm.BalancerModeOff {
				return pbm.BalancerModeOff
			}
		case <-dn.C:
			break Loop
		}
	}
	if bs != nil {
		return pbm.BalancerMode("")
	}

	return bs.Mode
}

const maxReplicationLagTimeSec = 21

// NodeSuits checks if node can perform backup
func NodeSuits(node *pbm.Node, inf *pbm.NodeInfo) (bool, error) {
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
func Upload(ctx context.Context, src Source, dst storage.Storage, compression pbm.CompressionType, compressLevel *int, fname string, sizeb int) (int64, error) {
	r, pw := io.Pipe()

	w, err := Compress(pw, compression, compressLevel)
	if err != nil {
		return 0, err
	}

	var rwErr rwErr
	var n int64
	go func() {
		n, rwErr.read = src.WriteTo(w)
		rwErr.compress = w.Close()
		pw.Close()
	}()

	saveDone := make(chan struct{})
	go func() {
		rwErr.write = dst.Save(fname, r, sizeb)
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

	if !rwErr.nil() {
		return 0, rwErr
	}

	return n, nil
}

func (b *Backup) reconcileStatus(bcpName, opid string, status pbm.Status, timeout *time.Duration) error {
	shards, err := b.cn.ClusterMembers()
	if err != nil {
		return errors.Wrap(err, "get cluster members")
	}

	if timeout != nil {
		return errors.Wrap(b.convergeClusterWithTimeout(bcpName, opid, shards, status, *timeout), "convergeClusterWithTimeout")
	}
	return errors.Wrap(b.convergeCluster(bcpName, opid, shards, status), "convergeCluster")
}

// convergeCluster waits until all given shards reached `status` and updates a cluster status
func (b *Backup) convergeCluster(bcpName, opid string, shards []pbm.Shard, status pbm.Status) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			ok, err := b.converged(bcpName, opid, shards, status)
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
func (b *Backup) convergeClusterWithTimeout(bcpName, opid string, shards []pbm.Shard, status pbm.Status, t time.Duration) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	tout := time.NewTicker(t)
	defer tout.Stop()
	for {
		select {
		case <-tk.C:
			ok, err := b.converged(bcpName, opid, shards, status)
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

func (b *Backup) converged(bcpName, opid string, shards []pbm.Shard, status pbm.Status) (bool, error) {
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
			if shard.Name == sh.RS {
				// check if node alive
				lock, err := b.cn.GetLockData(&pbm.LockHeader{
					Type:    pbm.CmdBackup,
					OPID:    opid,
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

func (b *Backup) waitForStatus(bcpName string, status pbm.Status, waitFor *time.Duration) error {
	var tout <-chan time.Time
	if waitFor != nil {
		tmr := time.NewTimer(*waitFor)
		defer tmr.Stop()
		tout = tmr.C
	}
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			bmeta, err := b.cn.GetBackupMeta(bcpName)
			if errors.Is(err, pbm.ErrNotFound) {
				continue
			}
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
				return errors.Errorf("cluster failed: %v", err)
			}
		case <-tout:
			return errors.New("no backup meta, looks like a leader failed to start")
		case <-b.cn.Context().Done():
			return nil
		}
	}
}

func (b *Backup) waitForFirstLastWrite(bcpName string) (first, last primitive.Timestamp, err error) {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			bmeta, err := b.cn.GetBackupMeta(bcpName)
			if err != nil {
				return first, last, errors.Wrap(err, "get backup metadata")
			}

			clusterTime, err := b.cn.ClusterTime()
			if err != nil {
				return first, last, errors.Wrap(err, "read cluster time")
			}

			if bmeta.Hb.T+pbm.StaleFrameSec < clusterTime.T {
				return first, last, errors.Errorf("backup stuck, last beat ts: %d", bmeta.Hb.T)
			}

			if bmeta.FirstWriteTS.T > 0 && bmeta.LastWriteTS.T > 0 {
				return bmeta.FirstWriteTS, bmeta.LastWriteTS, nil
			}
		case <-b.cn.Context().Done():
			return first, last, nil
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

func (b *Backup) setClusterFirstWrite(bcpName string) error {
	bmeta, err := b.cn.GetBackupMeta(bcpName)
	if err != nil {
		return errors.Wrap(err, "get backup metadata")
	}

	var fw primitive.Timestamp
	for _, rs := range bmeta.Replsets {
		if fw.T == 0 || primitive.CompareTimestamp(fw, rs.FirstWriteTS) == 1 {
			fw = rs.FirstWriteTS
		}
	}

	err = b.cn.SetFirstWrite(bcpName, fw)
	return errors.Wrap(err, "set timestamp")
}

func (b *Backup) setClusterLastWrite(bcpName string) error {
	bmeta, err := b.cn.GetBackupMeta(bcpName)
	if err != nil {
		return errors.Wrap(err, "get backup metadata")
	}

	var lw primitive.Timestamp
	for _, rs := range bmeta.Replsets {
		if primitive.CompareTimestamp(lw, rs.LastWriteTS) == -1 {
			lw = rs.LastWriteTS
		}
	}

	err = b.cn.SetLastWrite(bcpName, lw)
	return errors.Wrap(err, "set timestamp")
}
