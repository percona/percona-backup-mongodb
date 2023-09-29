package backup

import (
	"bytes"
	"encoding/json"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/lock"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/query"
	"github.com/percona/percona-backup-mongodb/internal/storage"
	"github.com/percona/percona-backup-mongodb/internal/topo"
	"github.com/percona/percona-backup-mongodb/internal/types"
	"github.com/percona/percona-backup-mongodb/internal/util"
	"github.com/percona/percona-backup-mongodb/internal/version"
	"github.com/percona/percona-backup-mongodb/pbm"
)

type Backup struct {
	cn       *pbm.PBM
	node     *pbm.Node
	typ      defs.BackupType
	incrBase bool
	timeouts *config.BackupTimeouts
}

func New(cn *pbm.PBM, node *pbm.Node) *Backup {
	return &Backup{
		cn:   cn,
		node: node,
		typ:  defs.LogicalBackup,
	}
}

func NewPhysical(cn *pbm.PBM, node *pbm.Node) *Backup {
	return &Backup{
		cn:   cn,
		node: node,
		typ:  defs.PhysicalBackup,
	}
}

func NewExternal(cn *pbm.PBM, node *pbm.Node) *Backup {
	return &Backup{
		cn:   cn,
		node: node,
		typ:  defs.ExternalBackup,
	}
}

func NewIncremental(cn *pbm.PBM, node *pbm.Node, base bool) *Backup {
	return &Backup{
		cn:       cn,
		node:     node,
		typ:      defs.IncrementalBackup,
		incrBase: base,
	}
}

func (b *Backup) SetTimeouts(t *config.BackupTimeouts) {
	b.timeouts = t
}

func (b *Backup) Init(
	ctx context.Context,
	bcp *types.BackupCmd,
	opid types.OPID,
	inf *topo.NodeInfo,
	store config.StorageConf,
	balancer topo.BalancerMode,
	l *log.Event,
) error {
	ts, err := topo.GetClusterTime(ctx, b.cn.Conn)
	if err != nil {
		return errors.Wrap(err, "read cluster time")
	}

	meta := &types.BackupMeta{
		Type:        b.typ,
		OPID:        opid.String(),
		Name:        bcp.Name,
		Namespaces:  bcp.Namespaces,
		Compression: bcp.Compression,
		Store:       store,
		StartTS:     time.Now().Unix(),
		Status:      defs.StatusStarting,
		Replsets:    []types.BackupReplset{},
		// the driver (mongo?) sets TS to the current wall clock if TS was 0, so have to init with 1
		LastWriteTS: primitive.Timestamp{T: 1, I: 1},
		// the driver (mongo?) sets TS to the current wall clock if TS was 0, so have to init with 1
		FirstWriteTS:   primitive.Timestamp{T: 1, I: 1},
		PBMVersion:     version.Current().Version,
		Nomination:     []types.BackupRsNomination{},
		BalancerStatus: balancer,
		Hb:             ts,
	}

	cfg, err := config.GetConfig(ctx, b.cn.Conn)
	if err != nil {
		return errors.Wrap(err, "unable to get PBM config settings")
	}
	_, err = util.StorageFromConfig(cfg, l)
	if errors.Is(err, util.ErrStorageUndefined) {
		return errors.New("backups cannot be saved because PBM storage configuration hasn't been set yet")
	}
	meta.Store = cfg.Storage

	ver, err := version.GetMongoVersion(ctx, b.node.Session())
	if err != nil {
		return errors.Wrap(err, "get mongo version")
	}
	meta.MongoVersion = ver.VersionString

	fcv, err := version.GetFCV(ctx, b.node.Session())
	if err != nil {
		return errors.Wrap(err, "get featureCompatibilityVersion")
	}
	meta.FCV = fcv

	if inf.IsSharded() {
		ss, err := b.cn.GetShards(ctx)
		if err != nil {
			return errors.Wrap(err, "get shards")
		}

		shards := make(map[string]string)
		for i := range ss {
			s := &ss[i]
			if s.RS != s.ID {
				shards[s.RS] = s.ID
			}
		}
		if len(shards) != 0 {
			meta.ShardRemap = shards
		}
	}

	return query.SetBackupMeta(ctx, b.cn.Conn, meta)
}

// Run runs backup.
// TODO: describe flow
//
//nolint:nonamedreturns
func (b *Backup) Run(ctx context.Context, bcp *types.BackupCmd, opid types.OPID, l *log.Event) (err error) {
	inf, err := topo.GetNodeInfoExt(ctx, b.node.Session())
	if err != nil {
		return errors.Wrap(err, "get cluster info")
	}

	rsMeta := types.BackupReplset{
		Name:         inf.SetName,
		Node:         inf.Me,
		StartTS:      time.Now().UTC().Unix(),
		Status:       defs.StatusRunning,
		Conditions:   []types.Condition{},
		FirstWriteTS: primitive.Timestamp{T: 1, I: 1},
	}
	if v := inf.IsConfigSrv(); v {
		rsMeta.IsConfigSvr = &v
	}

	stg, err := util.GetStorage(ctx, b.cn.Conn, l)
	if err != nil {
		return errors.Wrap(err, "unable to get PBM storage configuration settings")
	}

	bcpm, err := query.GetBackupMeta(ctx, b.cn.Conn, bcp.Name)
	if err != nil {
		return errors.Wrap(err, "balancer status, get backup meta")
	}

	// on any error the RS' and the backup' (in case this is the backup leader) meta will be marked appropriately
	defer func() {
		if err != nil {
			status := defs.StatusError
			if errors.Is(err, storage.ErrCancelled) || errors.Is(err, context.Canceled) {
				status = defs.StatusCancelled
			}

			ferr := query.ChangeRSState(b.cn.Conn, bcp.Name, rsMeta.Name, status, err.Error())
			l.Info("mark RS as %s `%v`: %v", status, err, ferr)

			if inf.IsLeader() {
				ferr := query.ChangeBackupState(b.cn.Conn, bcp.Name, status, err.Error())
				l.Info("mark backup as %s `%v`: %v", status, err, ferr)
			}
		}

		// Turn the balancer back on if needed
		//
		// Every agent will check if the balancer was on before the backup started.
		// And will try to turn it on again if so. So if the leader node went down after turning off
		// the balancer some other node will bring it back.
		// TODO: what if all agents went down.
		if bcpm.BalancerStatus != topo.BalancerModeOn {
			return
		}

		errd := topo.SetBalancerStatus(context.Background(), b.cn.Conn, topo.BalancerModeOn)
		if errd != nil {
			l.Error("set balancer ON: %v", errd)
			return
		}
		l.Debug("set balancer on")
	}()

	if inf.IsLeader() {
		hbstop := make(chan struct{})
		defer close(hbstop)

		err := query.BackupHB(ctx, b.cn.Conn, bcp.Name)
		if err != nil {
			return errors.Wrap(err, "init heartbeat")
		}

		go func() {
			tk := time.NewTicker(time.Second * 5)
			defer tk.Stop()

			for {
				select {
				case <-tk.C:
					err := query.BackupHB(ctx, b.cn.Conn, bcp.Name)
					if err != nil {
						l.Error("send pbm heartbeat: %v", err)
					}
				case <-hbstop:
					return
				}
			}
		}()

		if bcpm.BalancerStatus == topo.BalancerModeOn {
			err = topo.SetBalancerStatus(ctx, b.cn.Conn, topo.BalancerModeOff)
			if err != nil {
				return errors.Wrap(err, "set balancer OFF")
			}

			l.Debug("waiting for balancer off")
			bs := waitForBalancerOff(ctx, b.cn, time.Second*30, l)
			l.Debug("balancer status: %s", bs)
		}
	}

	// Waiting for StatusStarting to move further.
	// In case some preparations has to be done before backup.
	err = b.waitForStatus(ctx, bcp.Name, defs.StatusStarting, ref(b.timeouts.StartingStatus()))
	if err != nil {
		return errors.Wrap(err, "waiting for start")
	}

	defer func() {
		if !inf.IsLeader() {
			return
		}
		if !errors.Is(err, storage.ErrCancelled) || !errors.Is(err, context.Canceled) {
			return
		}

		if err := b.cn.DeleteBackupFiles(bcpm, stg); err != nil {
			l.Error("Failed to delete leftover files for canceled backup %q", bcpm.Name)
		}
	}()

	switch b.typ {
	case defs.LogicalBackup:
		err = b.doLogical(ctx, bcp, opid, &rsMeta, inf, stg, l)
	case defs.PhysicalBackup, defs.IncrementalBackup, defs.ExternalBackup:
		err = b.doPhysical(ctx, bcp, opid, &rsMeta, inf, stg, l)
	default:
		return errors.New("undefined backup type")
	}
	if err != nil {
		return err
	}

	err = query.ChangeRSState(b.cn.Conn, bcp.Name, rsMeta.Name, defs.StatusDone, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDone")
	}

	if inf.IsLeader() {
		epch, err := config.ResetEpochWithContext(ctx, b.cn.Conn)
		if err != nil {
			l.Error("reset epoch")
		} else {
			l.Debug("epoch set to %v", epch)
		}

		err = b.reconcileStatus(ctx, bcp.Name, opid.String(), defs.StatusDone, nil)
		if err != nil {
			return errors.Wrap(err, "check cluster for backup done")
		}

		bcpm, err = query.GetBackupMeta(ctx, b.cn.Conn, bcp.Name)
		if err != nil {
			return errors.Wrap(err, "get backup metadata")
		}

		err = writeMeta(stg, bcpm)
		if err != nil {
			return errors.Wrap(err, "dump metadata")
		}
	}

	// to be sure the locks released only after the "done" status had written
	err = b.waitForStatus(ctx, bcp.Name, defs.StatusDone, nil)
	return errors.Wrap(err, "waiting for done")
}

func waitForBalancerOff(ctx context.Context, cn *pbm.PBM, t time.Duration, l *log.Event) topo.BalancerMode {
	dn := time.NewTimer(t)
	defer dn.Stop()

	tk := time.NewTicker(time.Millisecond * 500)
	defer tk.Stop()

	var bs *topo.BalancerStatus
	var err error

Loop:
	for {
		select {
		case <-tk.C:
			bs, err = topo.GetBalancerStatus(ctx, cn.Conn)
			if err != nil {
				l.Error("get balancer status: %v", err)
				continue
			}
			if bs.Mode == topo.BalancerModeOff {
				return topo.BalancerModeOff
			}
		case <-dn.C:
			break Loop
		}
	}

	if bs != nil {
		return topo.BalancerMode("")
	}

	return bs.Mode
}

func (b *Backup) toState(
	ctx context.Context,
	status defs.Status,
	bcp, opid string,
	inf *topo.NodeInfo,
	wait *time.Duration,
) error {
	err := query.ChangeRSState(b.cn.Conn, bcp, inf.SetName, status, "")
	if err != nil {
		return errors.Wrap(err, "set shard's status")
	}

	if inf.IsLeader() {
		err = b.reconcileStatus(ctx, bcp, opid, status, wait)
		if err != nil {
			if errors.Is(err, errConvergeTimeOut) {
				return errors.Wrap(err, "couldn't get response from all shards")
			}
			return errors.Wrapf(err, "check cluster for backup `%s`", status)
		}
	}

	err = b.waitForStatus(ctx, bcp, status, wait)
	if err != nil {
		return errors.Wrapf(err, "waiting for %s", status)
	}

	return nil
}

func (b *Backup) reconcileStatus(
	ctx context.Context,
	bcpName, opid string,
	status defs.Status,
	timeout *time.Duration,
) error {
	shards, err := topo.ClusterMembers(ctx, b.cn.Conn.MongoClient())
	if err != nil {
		return errors.Wrap(err, "get cluster members")
	}

	if timeout != nil {
		return errors.Wrap(b.convergeClusterWithTimeout(ctx, bcpName, opid, shards, status, *timeout),
			"convergeClusterWithTimeout")
	}
	return errors.Wrap(b.convergeCluster(ctx, bcpName, opid, shards, status), "convergeCluster")
}

// convergeCluster waits until all given shards reached `status` and updates a cluster status
func (b *Backup) convergeCluster(
	ctx context.Context,
	bcpName, opid string,
	shards []topo.Shard,
	status defs.Status,
) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			ok, err := b.converged(ctx, bcpName, opid, shards, status)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}

var errConvergeTimeOut = errors.New("reached converge timeout")

// convergeClusterWithTimeout waits up to the geiven timeout until
// all given shards reached `status` and then updates the cluster status
func (b *Backup) convergeClusterWithTimeout(
	ctx context.Context,
	bcpName,
	opid string,
	shards []topo.Shard,
	status defs.Status,
	t time.Duration,
) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	tout := time.NewTicker(t)
	defer tout.Stop()

	for {
		select {
		case <-tk.C:
			ok, err := b.converged(ctx, bcpName, opid, shards, status)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		case <-tout.C:
			return errConvergeTimeOut
		case <-ctx.Done():
			return nil
		}
	}
}

func (b *Backup) converged(
	ctx context.Context,
	bcpName, opid string,
	shards []topo.Shard,
	status defs.Status,
) (bool, error) {
	shardsToFinish := len(shards)
	bmeta, err := query.GetBackupMeta(ctx, b.cn.Conn, bcpName)
	if err != nil {
		return false, errors.Wrap(err, "get backup metadata")
	}

	clusterTime, err := topo.GetClusterTime(ctx, b.cn.Conn)
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	for _, sh := range shards {
		for _, shard := range bmeta.Replsets {
			if shard.Name == sh.RS {
				// check if node alive
				lck, err := lock.GetLockData(ctx, b.cn.Conn, &lock.LockHeader{
					Type:    defs.CmdBackup,
					OPID:    opid,
					Replset: shard.Name,
				})

				// nodes are cleaning its locks moving to the done status
				// so no lock is ok and no need to ckech the heartbeats
				if status != defs.StatusDone && !errors.Is(err, mongo.ErrNoDocuments) {
					if err != nil {
						return false, errors.Wrapf(err, "unable to read lock for shard %s", shard.Name)
					}
					if lck.Heartbeat.T+defs.StaleFrameSec < clusterTime.T {
						return false, errors.Errorf("lost shard %s, last beat ts: %d", shard.Name, lck.Heartbeat.T)
					}
				}

				// check status
				switch shard.Status {
				case status:
					shardsToFinish--
				case defs.StatusCancelled:
					return false, storage.ErrCancelled
				case defs.StatusError:
					return false, errors.Errorf("backup on shard %s failed with: %s", shard.Name, bmeta.Error())
				}
			}
		}
	}

	if shardsToFinish == 0 {
		err := query.ChangeBackupState(b.cn.Conn, bcpName, status, "")
		if err != nil {
			return false, errors.Wrapf(err, "update backup meta with %s", status)
		}
		return true, nil
	}

	return false, nil
}

func (b *Backup) waitForStatus(
	ctx context.Context,
	bcpName string,
	status defs.Status,
	waitFor *time.Duration,
) error {
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
			bmeta, err := query.GetBackupMeta(ctx, b.cn.Conn, bcpName)
			if errors.Is(err, errors.ErrNotFound) {
				continue
			}
			if err != nil {
				return errors.Wrap(err, "get backup metadata")
			}

			clusterTime, err := topo.GetClusterTime(ctx, b.cn.Conn)
			if err != nil {
				return errors.Wrap(err, "read cluster time")
			}

			if bmeta.Hb.T+defs.StaleFrameSec < clusterTime.T {
				return errors.Errorf("backup stuck, last beat ts: %d", bmeta.Hb.T)
			}

			switch bmeta.Status {
			case status:
				return nil
			case defs.StatusCancelled:
				return storage.ErrCancelled
			case defs.StatusError:
				return errors.Errorf("cluster failed: %v", err)
			}
		case <-tout:
			return errors.New("no backup meta, looks like a leader failed to start")
		case <-ctx.Done():
			return nil
		}
	}
}

//nolint:nonamedreturns
func (b *Backup) waitForFirstLastWrite(
	ctx context.Context,
	bcpName string,
) (first, last primitive.Timestamp, err error) {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			bmeta, err := query.GetBackupMeta(ctx, b.cn.Conn, bcpName)
			if err != nil {
				return first, last, errors.Wrap(err, "get backup metadata")
			}

			clusterTime, err := topo.GetClusterTime(ctx, b.cn.Conn)
			if err != nil {
				return first, last, errors.Wrap(err, "read cluster time")
			}

			if bmeta.Hb.T+defs.StaleFrameSec < clusterTime.T {
				return first, last, errors.Errorf("backup stuck, last beat ts: %d", bmeta.Hb.T)
			}

			if bmeta.FirstWriteTS.T > 0 && bmeta.LastWriteTS.T > 0 {
				return bmeta.FirstWriteTS, bmeta.LastWriteTS, nil
			}
		case <-ctx.Done():
			return first, last, nil
		}
	}
}

func writeMeta(stg storage.Storage, meta *types.BackupMeta) error {
	b, err := json.MarshalIndent(meta, "", "\t")
	if err != nil {
		return errors.Wrap(err, "marshal data")
	}

	err = stg.Save(meta.Name+defs.MetadataFileSuffix, bytes.NewReader(b), -1)
	return errors.Wrap(err, "write to store")
}

func (b *Backup) setClusterFirstWrite(ctx context.Context, bcpName string) error {
	bmeta, err := query.GetBackupMeta(ctx, b.cn.Conn, bcpName)
	if err != nil {
		return errors.Wrap(err, "get backup metadata")
	}

	var fw primitive.Timestamp
	for _, rs := range bmeta.Replsets {
		if fw.T == 0 || fw.Compare(rs.FirstWriteTS) == 1 {
			fw = rs.FirstWriteTS
		}
	}

	err = query.SetFirstWrite(ctx, b.cn.Conn, bcpName, fw)
	return errors.Wrap(err, "set timestamp")
}

func (b *Backup) setClusterLastWrite(ctx context.Context, bcpName string) error {
	bmeta, err := query.GetBackupMeta(ctx, b.cn.Conn, bcpName)
	if err != nil {
		return errors.Wrap(err, "get backup metadata")
	}

	var lw primitive.Timestamp
	for _, rs := range bmeta.Replsets {
		if lw.Compare(rs.LastWriteTS) == -1 {
			lw = rs.LastWriteTS
		}
	}

	err = query.SetLastWrite(ctx, b.cn.Conn, bcpName, lw)
	return errors.Wrap(err, "set timestamp")
}

func ref[T any](v T) *T {
	return &v
}
