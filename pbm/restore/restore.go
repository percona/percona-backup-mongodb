package restore

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"

	mlog "github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/mongorestore"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/version"
)

func init() {
	// set date format for mongo tools (mongodump/mongorestore) logger
	//
	// duplicated in backup/restore packages just
	// in the sake of clarity
	mlog.SetDateFormat(log.LogTimeFormat)
	mlog.SetVerbosity(&options.Verbosity{
		VLevel: mlog.DebugLow,
	})
}

var excludeFromRestore = []string{
	pbm.DB + "." + pbm.CmdStreamCollection,
	pbm.DB + "." + pbm.LogCollection,
	pbm.DB + "." + pbm.ConfigCollection,
	pbm.DB + "." + pbm.BcpCollection,
	pbm.DB + "." + pbm.BcpOldCollection,
	pbm.DB + "." + pbm.RestoresCollection,
	pbm.DB + "." + pbm.LockCollection,
	pbm.DB + "." + pbm.LockOpCollection,
	pbm.DB + "." + pbm.PITRChunksCollection,
	pbm.DB + "." + pbm.PITRChunksOldCollection,
	pbm.DB + "." + pbm.AgentsStatusCollection,
	pbm.DB + "." + pbm.PBMOpLogCollection,
	"config.version",
	"config.mongos",
	"config.lockpings",
	"config.locks",
	"config.system.sessions",
	"config.cache.*",
	"config.shards",
	"config.transactions",
	"config.transaction_coordinators",
	"admin.system.version",
	"config.system.indexBuilds",
}

var excludeFromOplog = []string{
	"config.rangeDeletions",
	pbm.DB + "." + pbm.TmpUsersCollection,
	pbm.DB + "." + pbm.TmpRolesCollection,
}

type Restore struct {
	name     string
	cn       *pbm.PBM
	node     *pbm.Node
	stopHB   chan struct{}
	nodeInfo *pbm.NodeInfo
	stg      storage.Storage
	// Shards to participate in restore. Num of shards in bcp could
	// be less than in the cluster and this is ok. Only these shards
	// would be expected to run restore (distributed transactions sync,
	//  status checks etc.)
	//
	// Only the restore leader would have this info.
	shards []pbm.Shard

	oplog *Oplog
	log   *log.Event
	opid  string
}

// New creates a new restore object
func New(cn *pbm.PBM, node *pbm.Node) *Restore {
	return &Restore{
		cn:   cn,
		node: node,
	}
}

// Close releases object resources.
// Should be run to avoid leaks.
func (r *Restore) Close() {
	if r.stopHB != nil {
		close(r.stopHB)
	}
}

func (r *Restore) exit(err error, l *log.Event) {
	if err != nil && !errors.Is(err, ErrNoDataForShard) {
		ferr := r.MarkFailed(err)
		if ferr != nil {
			l.Error("mark restore as failed `%v`: %v", err, ferr)
		}
	}

	r.Close()
}

// Snapshot do the snapshot's (mongo dump) restore
func (r *Restore) Snapshot(cmd pbm.RestoreCmd, opid pbm.OPID, l *log.Event) (err error) {
	defer r.exit(err, l)

	err = r.init(cmd.Name, opid, l)
	if err != nil {
		return err
	}

	err = r.cn.SetRestoreBackup(r.name, cmd.BackupName)
	if err != nil {
		return errors.Wrap(err, "set backup name")
	}

	bcp, err := r.SnapshotMeta(cmd.BackupName)
	if err != nil {
		return err
	}

	dump, oplog, err := r.snapshotObjects(bcp)
	if err != nil {
		return err
	}

	err = r.toState(pbm.StatusRunning, &pbm.WaitActionStart)
	if err != nil {
		return err
	}

	err = r.RunSnapshot(dump, bcp)
	if err != nil {
		return err
	}

	err = r.toState(pbm.StatusDumpDone, nil)
	if err != nil {
		return err
	}

	err = r.applyOplog([]pbm.OplogChunk{{
		RS:          r.nodeInfo.SetName,
		FName:       oplog,
		Compression: bcp.Compression,
		StartTS:     bcp.FirstWriteTS,
		EndTS:       bcp.LastWriteTS,
	}}, nil, nil)
	if err != nil {
		return err
	}

	return r.Done()
}

// PITR do the Point-in-Time Recovery
func (r *Restore) PITR(cmd pbm.PITRestoreCmd, opid pbm.OPID, l *log.Event) (err error) {
	defer r.exit(err, l)

	err = r.init(cmd.Name, opid, l)
	if err != nil {
		return err
	}

	tsTo := primitive.Timestamp{T: uint32(cmd.TS), I: uint32(cmd.I)}
	if r.nodeInfo.IsLeader() {
		err = r.cn.SetOplogTimestamps(r.name, 0, int64(tsTo.T))
		if err != nil {
			return errors.Wrap(err, "set PITR timestamp")
		}
	}

	var bcp *pbm.BackupMeta
	if cmd.Bcp == "" {
		bcp, err = r.cn.GetLastBackup(&tsTo)
		if errors.Is(err, pbm.ErrNotFound) {
			return errors.Errorf("no backup found before ts %v", tsTo)
		}
		if err != nil {
			return errors.Wrap(err, "define last backup")
		}
	} else {
		bcp, err = r.SnapshotMeta(cmd.Bcp)
		if err != nil {
			return err
		}
		if primitive.CompareTimestamp(bcp.LastWriteTS, tsTo) >= 0 {
			return errors.New("snapshot's last write is later than the target time. Try to set an earlier snapshot. Or leave the snapshot empty so PBM will choose one.")
		}
	}

	err = r.cn.SetRestoreBackup(r.name, bcp.Name)
	if err != nil {
		return errors.Wrap(err, "set backup name")
	}

	chunks, err := r.chunks(bcp.LastWriteTS, tsTo)
	if err != nil {
		return err
	}

	dump, oplog, err := r.snapshotObjects(bcp)
	if err != nil {
		return err
	}

	err = r.toState(pbm.StatusRunning, &pbm.WaitActionStart)
	if err != nil {
		return err
	}

	err = r.RunSnapshot(dump, bcp)
	if err != nil {
		return err
	}

	err = r.toState(pbm.StatusDumpDone, nil)
	if err != nil {
		return err
	}

	snapshotChunk := pbm.OplogChunk{
		RS:          r.nodeInfo.SetName,
		FName:       oplog,
		Compression: bcp.Compression,
		StartTS:     bcp.FirstWriteTS,
		EndTS:       bcp.LastWriteTS,
	}

	err = r.applyOplog(append([]pbm.OplogChunk{snapshotChunk}, chunks...), nil, &tsTo)
	if err != nil {
		return err
	}

	return r.Done()
}

func (r *Restore) ReplayOplog(cmd pbm.ReplayCmd, opid pbm.OPID, l *log.Event) (err error) {
	defer r.exit(err, l)

	if err = r.init(cmd.Name, opid, l); err != nil {
		return errors.Wrap(err, "init")
	}

	if !r.nodeInfo.IsPrimary {
		return errors.Errorf("%q is not primary", r.nodeInfo.SetName)
	}

	r.shards, err = r.cn.ClusterMembers()
	if err != nil {
		return errors.Wrap(err, "get cluster members")
	}

	if r.nodeInfo.IsLeader() {
		err := r.cn.SetOplogTimestamps(r.name, int64(cmd.Start.T), int64(cmd.End.T))
		if err != nil {
			return errors.Wrap(err, "set oplog timestamps")
		}
	}

	chunks, err := r.chunks(cmd.Start, cmd.End)
	if err != nil {
		return err
	}

	err = r.toState(pbm.StatusRunning, &pbm.WaitActionStart)
	if err != nil {
		return err
	}

	err = r.applyOplog(chunks, &cmd.Start, &cmd.End)
	if err != nil {
		return err
	}

	return r.Done()
}

func (r *Restore) init(name string, opid pbm.OPID, l *log.Event) (err error) {
	r.log = l

	r.nodeInfo, err = r.node.GetInfo()
	if err != nil {
		return errors.Wrap(err, "get node data")
	}
	if r.nodeInfo.SetName == "" {
		return errors.Wrap(err, "unable to define replica set")
	}

	r.name = name

	r.opid = opid.String()
	if r.nodeInfo.IsLeader() {
		ts, err := r.cn.ClusterTime()
		if err != nil {
			return errors.Wrap(err, "init restore meta, read cluster time")
		}

		meta := &pbm.RestoreMeta{
			OPID:     r.opid,
			Name:     r.name,
			StartTS:  time.Now().Unix(),
			Status:   pbm.StatusStarting,
			Replsets: []pbm.RestoreReplset{},
			Hb:       ts,
		}
		err = r.cn.SetRestoreMeta(meta)
		if err != nil {
			return errors.Wrap(err, "write backup meta to db")
		}

		r.stopHB = make(chan struct{})
		go func() {
			tk := time.NewTicker(time.Second * 5)
			defer tk.Stop()
			for {
				select {
				case <-tk.C:
					err := r.cn.RestoreHB(r.name)
					if err != nil {
						l.Error("send heartbeat: %v", err)
					}
				case <-r.stopHB:
					return
				}
			}
		}()
	}

	// Waiting for StatusStarting to move further.
	// In case some preparations has to be done before the restore.
	err = r.waitForStatus(pbm.StatusStarting)
	if err != nil {
		return errors.Wrap(err, "waiting for start")
	}

	rsMeta := pbm.RestoreReplset{
		Name:       r.nodeInfo.SetName,
		StartTS:    time.Now().UTC().Unix(),
		Status:     pbm.StatusStarting,
		Conditions: []pbm.Condition{},
	}

	err = r.cn.AddRestoreRSMeta(r.name, rsMeta)
	if err != nil {
		return errors.Wrap(err, "add shard's metadata")
	}

	r.stg, err = r.cn.GetStorage(r.log)
	if err != nil {
		return errors.Wrap(err, "get backup storage")
	}

	return nil
}

// chunks defines chunks of oplog slice in given range, ensures its integrity (timeline
// is contiguous - there are no gaps), checks for respective files on storage and returns
// chunks list if all checks passed
func (r *Restore) chunks(from, to primitive.Timestamp) ([]pbm.OplogChunk, error) {
	chunks, err := r.cn.PITRGetChunksSlice(r.nodeInfo.SetName, from, to)
	if err != nil {
		return nil, errors.Wrap(err, "get chunks index")
	}

	if len(chunks) == 0 {
		return nil, errors.New("no chunks found")
	}

	if primitive.CompareTimestamp(chunks[len(chunks)-1].EndTS, to) == -1 {
		return nil, errors.Errorf("no chunk with the target time, the last chunk ends on %v", chunks[len(chunks)-1].EndTS)
	}

	last := from
	for _, c := range chunks {
		if primitive.CompareTimestamp(last, c.StartTS) == -1 {
			return nil, errors.Errorf("integrity vilolated, expect chunk with start_ts %v, but got %v", last, c.StartTS)
		}
		last = c.EndTS

		_, err := r.stg.FileStat(c.FName)
		if err != nil {
			return nil, errors.Errorf("failed to ensure chunk %v.%v on the storage, file: %s, error: %v", c.StartTS, c.EndTS, c.FName, err)
		}
	}

	return chunks, nil
}

func (r *Restore) SnapshotMeta(backupName string) (bcp *pbm.BackupMeta, err error) {
	bcp, err = r.cn.GetBackupMeta(backupName)
	if errors.Is(err, pbm.ErrNotFound) {
		bcp, err = r.getMetaFromStore(backupName)
	}
	if err != nil {
		return nil, errors.Wrap(err, "get backup metadata")
	}

	return bcp, err
}

// setShards defines and set shards participating in the restore
// cluster migth have more shards then the backup and it's ok. But all
// backup's shards must have respective destination on the target cluster.
func (r *Restore) setShards(bcp *pbm.BackupMeta) error {
	s, err := r.cn.ClusterMembers()
	if err != nil {
		return errors.Wrap(err, "get cluster members")
	}

	fl := make(map[string]pbm.Shard, len(s))
	for _, rs := range s {
		fl[rs.RS] = rs
	}

	var nors []string
	for _, sh := range bcp.Replsets {
		rs, ok := fl[sh.Name]
		if !ok {
			nors = append(nors, sh.Name)
			continue
		}

		r.shards = append(r.shards, rs)
	}

	if r.nodeInfo.IsLeader() && len(nors) > 0 {
		return errors.Errorf("extra/unknown replica set found in the backup: %s", strings.Join(nors, ","))
	}

	return nil
}

var ErrNoDataForShard = errors.New("no data for shard")

func (r *Restore) snapshotObjects(bcp *pbm.BackupMeta) (dump, oplog string, err error) {
	var ok bool
	for _, v := range bcp.Replsets {
		if v.Name == r.nodeInfo.SetName {
			dump = v.DumpName
			oplog = v.OplogName
			ok = true
			break
		}
	}
	if !ok {
		if r.nodeInfo.IsLeader() {
			return "", "", errors.New("no data for the config server or sole rs in backup")
		}
		return "", "", ErrNoDataForShard
	}

	_, err = r.stg.FileStat(dump)
	if err != nil {
		return "", "", errors.Errorf("failed to ensure snapshot file %s: %v", dump, err)
	}

	_, err = r.stg.FileStat(oplog)
	if err != nil {
		return "", "", errors.Errorf("failed to ensure oplog file %s: %v", oplog, err)
	}

	return dump, oplog, nil
}

func (r *Restore) checkSnapshot(bcp *pbm.BackupMeta) error {
	if bcp.Status != pbm.StatusDone {
		return errors.Errorf("backup wasn't successful: status: %s, error: %s", bcp.Status, bcp.Error)
	}

	if !version.Compatible(version.DefaultInfo.Version, bcp.PBMVersion) {
		return errors.Errorf("backup version (v%s) is not compatible with PBM v%s", bcp.PBMVersion, version.DefaultInfo.Version)
	}

	return nil
}

const (
	preserveUUID = true

	batchSizeDefault           = 500
	numInsertionWorkersDefault = 10
)

func (r *Restore) toState(status pbm.Status, wait *time.Duration) error {
	r.log.Info("moving to state %s", status)
	err := r.cn.ChangeRestoreRSState(r.name, r.nodeInfo.SetName, status, "")
	if err != nil {
		return errors.Wrap(err, "set shard's status")
	}

	if r.nodeInfo.IsLeader() {
		err = r.reconcileStatus(status, wait)
		if err != nil {
			if errors.Cause(err) == errConvergeTimeOut {
				return errors.Wrap(err, "couldn't get response from all shards")
			}
			return errors.Wrap(err, "check cluster for restore started")
		}
	}

	err = r.waitForStatus(status)
	if err != nil {
		return errors.Wrapf(err, "waiting for %s", status)
	}

	return nil
}

func (r *Restore) RunSnapshot(dump string, bcp *pbm.BackupMeta) (err error) {
	err = r.checkSnapshot(bcp)
	if err != nil {
		return err
	}

	err = r.setShards(bcp)
	if err != nil {
		return err
	}

	sr, err := r.stg.SourceReader(dump)
	if err != nil {
		return errors.Wrapf(err, "get object %s for the storage", dump)
	}
	defer sr.Close()

	dumpReader, err := Decompress(sr, bcp.Compression)
	if err != nil {
		return errors.Wrapf(err, "decompress object %s", dump)
	}
	defer dumpReader.Close()

	// Restore snapshot (mongorestore)
	err = r.snapshot(dumpReader)
	if err != nil {
		return errors.Wrap(err, "mongorestore")
	}

	r.log.Info("restoring users and roles")
	cusr, err := r.node.CurrentUser()
	if err != nil {
		return errors.Wrap(err, "get current user")
	}

	err = r.swapUsers(r.cn.Context(), cusr)
	if err != nil {
		return errors.Wrap(err, "swap users 'n' roles")
	}

	err = pbm.DropTMPcoll(r.cn.Context(), r.node.Session())
	if err != nil {
		r.log.Warning("drop tmp collections: %v", err)
	}

	return nil
}

func (r *Restore) distTxnChecker(done <-chan struct{}, ctxn chan pbm.RestoreTxn, txnSyncErr chan<- error) {
	defer r.log.Debug("exit distTxnChecker")

	for {
		select {
		case txn := <-ctxn:
			r.log.Debug("found dist txn: %s", txn)
			err := r.cn.RestoreSetRSTxn(r.name, r.nodeInfo.SetName, txn)
			if err != nil {
				txnSyncErr <- errors.Wrapf(err, "set transaction %v", txn)
				return
			}

			if txn.State == pbm.TxnCommit {
				txn.State, err = r.txnWaitForShards(txn)
				if err != nil {
					txnSyncErr <- errors.Wrapf(err, "wait transaction %v", txn)
					return
				}
				r.log.Debug("txn converged to [%s] <%s>", txn.State, txn.ID)
				ctxn <- txn
			}
		case <-done:
			return
		}
	}
}

func (r *Restore) waitingTxnChecker(e *error, done <-chan struct{}) {
	defer r.log.Debug("exit waitingTxnChecker")

	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	observedTxn := make(map[string]struct{})
	for {
		select {
		case <-tk.C:
			err := r.checkWaitingTxns(observedTxn)
			if err != nil {
				e = &err
				return
			}
		case <-done:
			return
		}
	}
}

// In order to sync distributed transactions (commit ontly when all participated shards are committed),
// on all participated in the retore agents:
// distTxnChecker:
// - Receiving distributed transactions from the oplog applier, will add set it to the shards restore meta.
// - If txn's state is `commit` it will wait from all of the rest of the shards for:
// 		- either transaction committed on the shard (have `commitTransaction`);
//      - or shard didn't participate in the transaction (more on that below);
//      - or shard participated in the txn (have prepare ops) but no `commitTransaction` by the end of the oplog.
//    If any of the shards encounters the latter - the transaction is sent back to the applier as aborted.
// 	  Otherwise - committed.
// By looking at just transactions in the oplog we can't tell which shards were participating in it. So given that
// starting `opTime` of the transaction is the same on all shards, we can assume that if some shard(s) oplog applier
// observed greater than the txn's `opTime` and hadn't seen such txn - it wasn't part of this transaction at all. To
// communicate that, each agent runs `checkWaitingTxns` which in turn periodically checks restore metadata and sees
// any new (unobserved before) waiting for the transaction, posts last observed opTime. We go with `checkWaitingTxns`
// instead of just updating each observed `opTime`  since the latter would add an extra 1 write to each oplog op on
// sharded clusters even if there are no dist txns at all.
func (r *Restore) applyOplog(chunks []pbm.OplogChunk, start, end *primitive.Timestamp) error {
	r.log.Info("starting oplog replay")
	var err error

	mgoV, err := r.node.GetMongoVersion()
	if err != nil || len(mgoV.Version) < 1 {
		return errors.Wrap(err, "define mongo version")
	}

	var (
		ctxn       chan pbm.RestoreTxn
		txnSyncErr chan error
	)
	if r.nodeInfo.IsSharded() {
		ctxn = make(chan pbm.RestoreTxn)
		txnSyncErr = make(chan error)
	}

	r.oplog, err = NewOplog(r.node, mgoV, preserveUUID, ctxn, txnSyncErr)
	if err != nil {
		return errors.Wrap(err, "create oplog")
	}

	var startTS, endTS primitive.Timestamp
	if start != nil {
		startTS = *start
	}
	if end != nil {
		endTS = *end
	}
	r.oplog.SetTimeframe(startTS, endTS)

	var waitTxnErr error
	if r.nodeInfo.IsSharded() {
		r.log.Debug("starting sharded txn sync")
		if len(r.shards) == 0 {
			return errors.New("participating shards undefined")
		}
		done := make(chan struct{})
		go r.distTxnChecker(done, ctxn, txnSyncErr)
		go r.waitingTxnChecker(&waitTxnErr, done)
		defer close(done)
	}

	var lts primitive.Timestamp
	for _, chnk := range chunks {
		r.log.Debug("+ applying %v", chnk)

		lts, err = r.replyChunk(chnk.FName, chnk.Compression)
		if err != nil {
			return errors.Wrapf(err, "replay chunk %v.%v", chnk.StartTS.T, chnk.EndTS.T)
		}
		if waitTxnErr != nil {
			return errors.Wrap(err, "check waiting transactions")
		}
	}

	r.log.Info("oplog replay finished on %v", lts)

	return nil
}

func (r *Restore) checkWaitingTxns(observedTxn map[string]struct{}) error {
	rmeta, err := r.cn.GetRestoreMeta(r.name)
	if err != nil {
		return errors.Wrap(err, "get restore metadata")
	}

	for _, shard := range rmeta.Replsets {
		if _, ok := observedTxn[shard.Txn.ID]; shard.Txn.State == pbm.TxnCommit && !ok {
			ts := primitive.Timestamp{r.oplog.LastOpTS(), 0}
			if primitive.CompareTimestamp(ts, shard.Txn.Ctime) > 0 {
				err := r.cn.SetCurrentOp(r.name, r.nodeInfo.SetName, ts)
				if err != nil {
					return errors.Wrap(err, "set current op timestamp")
				}
			}

			observedTxn[shard.Txn.ID] = struct{}{}
			return nil
		}
	}

	return nil
}

func (r *Restore) txnWaitForShards(txn pbm.RestoreTxn) (pbm.TxnState, error) {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			s, err := r.checkTxn(txn)
			if err != nil {
				return pbm.TxnUnknown, err
			}
			if s == pbm.TxnCommit || s == pbm.TxnAbort {
				return s, nil
			}
		case <-r.cn.Context().Done():
			return pbm.TxnAbort, nil
		}
	}
}

func (r *Restore) checkTxn(txn pbm.RestoreTxn) (pbm.TxnState, error) {
	bmeta, err := r.cn.GetRestoreMeta(r.name)
	if err != nil {
		return pbm.TxnUnknown, errors.Wrap(err, "get restore metadata")
	}

	clusterTime, err := r.cn.ClusterTime()
	if err != nil {
		return pbm.TxnUnknown, errors.Wrap(err, "read cluster time")
	}

	// not going directly thru bmeta.Replsets to be sure we've heard back
	// from all participated in the restore shards.
	shardsToFinish := len(r.shards)
	for _, sh := range r.shards {
		for _, shard := range bmeta.Replsets {
			if shard.Name == sh.RS {
				// check if node alive
				lock, err := r.cn.GetLockData(&pbm.LockHeader{
					Type:    pbm.CmdRestore,
					OPID:    r.opid,
					Replset: shard.Name,
				})

				// nodes are cleaning its locks moving to the done status
				// so no lock is ok and not need to ckech the heartbeats
				if err != mongo.ErrNoDocuments {
					if err != nil {
						return pbm.TxnUnknown, errors.Wrapf(err, "unable to read lock for shard %s", shard.Name)
					}
					if lock.Heartbeat.T+pbm.StaleFrameSec < clusterTime.T {
						return pbm.TxnUnknown, errors.Errorf("lost shard %s, last beat ts: %d", shard.Name, lock.Heartbeat.T)
					}
				}

				if shard.Status == pbm.StatusError {
					return pbm.TxnUnknown, errors.Errorf("shard %s failed with: %v", shard.Name, shard.Error)
				}

				if primitive.CompareTimestamp(shard.CurrentOp, txn.Ctime) == 1 {
					shardsToFinish--
					continue
				}

				if shard.Txn.ID != txn.ID {
					if shard.Status == pbm.StatusDone ||
						primitive.CompareTimestamp(shard.Txn.Ctime, txn.Ctime) == 1 {
						shardsToFinish--
						continue
					}
					return pbm.TxnUnknown, nil
				}

				// check status
				switch shard.Txn.State {
				case pbm.TxnPrepare:
					if shard.Status == pbm.StatusDone {
						return pbm.TxnAbort, nil
					}
					return pbm.TxnUnknown, nil
				case pbm.TxnAbort:
					return pbm.TxnAbort, nil
				case pbm.TxnCommit:
					shardsToFinish--
				}
			}
		}
	}

	if shardsToFinish == 0 {
		return pbm.TxnCommit, nil
	}

	return pbm.TxnUnknown, nil
}

func (r *Restore) snapshot(input io.Reader) (err error) {
	topts := options.New("mongorestore", "0.0.1", "none", "", true, options.EnabledOptions{Auth: true, Connection: true, Namespace: true, URI: true})
	topts.URI, err = options.NewURI(r.node.ConnURI())
	if err != nil {
		return errors.Wrap(err, "parse connection string")
	}

	err = topts.NormalizeOptionsAndURI()
	if err != nil {
		return errors.Wrap(err, "parse opts")
	}

	topts.Direct = true
	topts.WriteConcern = writeconcern.New(writeconcern.WMajority())

	cfg, err := r.cn.GetConfig()
	if err != nil {
		return errors.Wrap(err, "unable to get PBM config settings")
	}

	batchSize := batchSizeDefault
	if cfg.Restore.BatchSize > 0 {
		batchSize = cfg.Restore.BatchSize
	}
	numInsertionWorkers := numInsertionWorkersDefault
	if cfg.Restore.NumInsertionWorkers > 0 {
		numInsertionWorkers = cfg.Restore.NumInsertionWorkers
	}

	mopts := mongorestore.Options{}
	mopts.ToolOptions = topts
	mopts.InputOptions = &mongorestore.InputOptions{
		Archive: "-",
	}
	mopts.OutputOptions = &mongorestore.OutputOptions{
		BulkBufferSize:           batchSize,
		BypassDocumentValidation: true,
		Drop:                     true,
		NumInsertionWorkers:      numInsertionWorkers,
		NumParallelCollections:   1,
		PreserveUUID:             preserveUUID,
		StopOnError:              true,
		WriteConcern:             "majority",
	}
	mopts.NSOptions = &mongorestore.NSOptions{
		NSExclude: excludeFromRestore,
	}

	mr, err := mongorestore.New(mopts)
	if err != nil {
		return errors.Wrap(err, "create mongorestore obj")
	}
	mr.SkipUsersAndRoles = true
	mr.InputReader = input

	rdumpResult := mr.Restore()
	mr.Close()
	if rdumpResult.Err != nil {
		return errors.Wrapf(rdumpResult.Err, "restore mongo dump (successes: %d / fails: %d)", rdumpResult.Successes, rdumpResult.Failures)
	}

	return nil
}

func (r *Restore) replyChunk(file string, c pbm.CompressionType) (lts primitive.Timestamp, err error) {
	or, err := r.stg.SourceReader(file)
	if err != nil {
		return lts, errors.Wrapf(err, "get object %s form the storage", file)
	}
	defer or.Close()

	oplogReader, err := Decompress(or, c)
	if err != nil {
		return lts, errors.Wrapf(err, "decompress object %s", file)
	}
	defer oplogReader.Close()

	lts, err = r.oplog.Apply(oplogReader)

	return lts, errors.Wrap(err, "apply oplog for chunk")
}

// Done waits for the replicas to finish the job
// and marks restore as done
func (r *Restore) Done() error {
	err := r.cn.ChangeRestoreRSState(r.name, r.nodeInfo.SetName, pbm.StatusDone, "")
	if err != nil {
		return errors.Wrap(err, "set shard's StatusDone")
	}

	if r.nodeInfo.IsLeader() {
		err = r.reconcileStatus(pbm.StatusDone, nil)
		if err != nil {
			return errors.Wrap(err, "check cluster for the restore done")
		}
	}

	return nil
}

func (r *Restore) swapUsers(ctx context.Context, exclude *pbm.AuthInfo) error {
	rolesC := r.node.Session().Database("admin").Collection("system.roles")

	eroles := []string{}
	for _, r := range exclude.UserRoles {
		eroles = append(eroles, r.DB+"."+r.Role)
	}

	curr, err := r.node.Session().Database(pbm.DB).Collection(pbm.TmpRolesCollection).Find(ctx, bson.M{"_id": bson.M{"$nin": eroles}})
	if err != nil {
		return errors.Wrap(err, "create cursor for tmpRoles")
	}
	defer curr.Close(ctx)
	_, err = rolesC.DeleteMany(ctx, bson.M{"_id": bson.M{"$nin": eroles}})
	if err != nil {
		return errors.Wrap(err, "delete current roles")
	}

	for curr.Next(ctx) {
		rl := new(interface{})
		err := curr.Decode(rl)
		if err != nil {
			return errors.Wrap(err, "decode role")
		}
		_, err = rolesC.InsertOne(ctx, rl)
		if err != nil {
			return errors.Wrap(err, "insert role")
		}
	}

	user := ""
	if len(exclude.Users) > 0 {
		user = exclude.Users[0].DB + "." + exclude.Users[0].User
	}
	cur, err := r.node.Session().Database(pbm.DB).Collection(pbm.TmpUsersCollection).Find(ctx, bson.M{"_id": bson.M{"$ne": user}})
	if err != nil {
		return errors.Wrap(err, "create cursor for tmpUsers")
	}
	defer cur.Close(ctx)

	usersC := r.node.Session().Database("admin").Collection("system.users")
	_, err = usersC.DeleteMany(ctx, bson.M{"_id": bson.M{"$ne": user}})
	if err != nil {
		return errors.Wrap(err, "delete current users")
	}

	for cur.Next(ctx) {
		u := new(interface{})
		err := cur.Decode(u)
		if err != nil {
			return errors.Wrap(err, "decode user")
		}
		_, err = usersC.InsertOne(ctx, u)
		if err != nil {
			return errors.Wrap(err, "insert user")
		}
	}

	return nil
}

func (r *Restore) reconcileStatus(status pbm.Status, timeout *time.Duration) error {
	if timeout != nil {
		return errors.Wrap(r.convergeClusterWithTimeout(status, *timeout), "convergeClusterWithTimeout")
	}
	return errors.Wrap(r.convergeCluster(status), "convergeCluster")
}

// convergeCluster waits until all participating shards reached `status` and updates a cluster status
func (r *Restore) convergeCluster(status pbm.Status) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			ok, err := r.converged(r.shards, status)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		case <-r.cn.Context().Done():
			return nil
		}
	}
}

var errConvergeTimeOut = errors.New("reached converge timeout")

// convergeClusterWithTimeout waits up to the geiven timeout until all participating shards reached
// `status` and then updates the cluster status
func (r *Restore) convergeClusterWithTimeout(status pbm.Status, t time.Duration) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	tout := time.NewTicker(t)
	defer tout.Stop()
	for {
		select {
		case <-tk.C:
			ok, err := r.converged(r.shards, status)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
		case <-tout.C:
			return errConvergeTimeOut
		case <-r.cn.Context().Done():
			return nil
		}
	}
}

func (r *Restore) converged(shards []pbm.Shard, status pbm.Status) (bool, error) {
	shardsToFinish := len(shards)
	bmeta, err := r.cn.GetRestoreMeta(r.name)
	if err != nil {
		return false, errors.Wrap(err, "get backup metadata")
	}

	clusterTime, err := r.cn.ClusterTime()
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	for _, sh := range shards {
		for _, shard := range bmeta.Replsets {
			if shard.Name == sh.RS {
				// check if node alive
				lock, err := r.cn.GetLockData(&pbm.LockHeader{
					Type:    pbm.CmdRestore,
					OPID:    r.opid,
					Replset: shard.Name,
				})

				// nodes are cleaning its locks moving to the done status
				// so no lock is ok and not need to ckech the heartbeats
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
				case pbm.StatusError:
					bmeta.Status = pbm.StatusError
					bmeta.Error = shard.Error
					return false, errors.Errorf("restore on the shard %s failed with: %s", shard.Name, shard.Error)
				}
			}
		}
	}

	if shardsToFinish == 0 {
		err := r.cn.ChangeRestoreState(r.name, status, "")
		if err != nil {
			return false, errors.Wrapf(err, "update backup meta with %s", status)
		}
		return true, nil
	}

	return false, nil
}

func (r *Restore) waitForStatus(status pbm.Status) error {
	r.log.Debug("waiting for '%s' status", status)
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for {
		select {
		case <-tk.C:
			meta, err := r.cn.GetRestoreMeta(r.name)
			if errors.Is(err, pbm.ErrNotFound) {
				continue
			}
			if err != nil {
				return errors.Wrap(err, "get restore metadata")
			}

			clusterTime, err := r.cn.ClusterTime()
			if err != nil {
				return errors.Wrap(err, "read cluster time")
			}

			if meta.Hb.T+pbm.StaleFrameSec < clusterTime.T {
				return errors.Errorf("restore stuck, last beat ts: %d", meta.Hb.T)
			}

			switch meta.Status {
			case status:
				return nil
			case pbm.StatusError:
				return errors.Errorf("cluster failed: %s", meta.Error)
			}
		case <-r.cn.Context().Done():
			return nil
		}
	}
}

// MarkFailed sets the restore and rs state as failed with the given message
func (r *Restore) MarkFailed(e error) error {
	err := r.cn.ChangeRestoreState(r.name, pbm.StatusError, e.Error())
	if err != nil {
		return errors.Wrap(err, "set restore state")
	}
	err = r.cn.ChangeRestoreRSState(r.name, r.nodeInfo.SetName, pbm.StatusError, e.Error())
	return errors.Wrap(err, "set replset state")
}

func (r *Restore) getMetaFromStore(bcpName string) (*pbm.BackupMeta, error) {
	rd, err := r.stg.SourceReader(bcpName + pbm.MetadataFileSuffix)
	if err != nil {
		return nil, errors.Wrap(err, "get from store")
	}
	defer rd.Close()

	b := &pbm.BackupMeta{}
	err = json.NewDecoder(rd).Decode(b)

	return b, errors.Wrap(err, "decode")
}
