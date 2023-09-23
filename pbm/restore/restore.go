package restore

import (
	"encoding/json"
	"time"

	"github.com/golang/snappy"
	"github.com/mongodb/mongo-tools/common/idx"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/internal/compress"
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
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
)

func GetMetaFromStore(stg storage.Storage, bcpName string) (*types.BackupMeta, error) {
	rd, err := stg.SourceReader(bcpName + defs.MetadataFileSuffix)
	if err != nil {
		return nil, errors.Wrap(err, "get from store")
	}
	defer rd.Close()

	b := &types.BackupMeta{}
	err = json.NewDecoder(rd).Decode(b)

	return b, errors.Wrap(err, "decode")
}

func toState(
	ctx context.Context,
	cn *pbm.PBM,
	status defs.Status,
	bcp string,
	inf *topo.NodeInfo,
	reconcileFn reconcileStatus,
	wait *time.Duration,
) error {
	err := query.ChangeRestoreRSState(ctx, cn.Conn, bcp, inf.SetName, status, "")
	if err != nil {
		return errors.Wrap(err, "set shard's status")
	}

	if inf.IsLeader() {
		err = reconcileFn(ctx, status, wait)
		if err != nil {
			if errors.Is(err, errConvergeTimeOut) {
				return errors.Wrap(err, "couldn't get response from all shards")
			}
			return errors.Wrapf(err, "check cluster for restore `%s`", status)
		}
	}

	err = waitForStatus(ctx, cn, bcp, status)
	if err != nil {
		return errors.Wrapf(err, "waiting for %s", status)
	}

	return nil
}

type reconcileStatus func(ctx context.Context, status defs.Status, timeout *time.Duration) error

// convergeCluster waits until all participating shards reached `status` and updates a cluster status
func convergeCluster(ctx context.Context, cn *pbm.PBM, name, opid string, shards []topo.Shard, status defs.Status) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			ok, err := converged(ctx, cn, name, opid, shards, status)
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

// convergeClusterWithTimeout waits up to the geiven timeout until all participating shards reached
// `status` and then updates the cluster status
func convergeClusterWithTimeout(
	ctx context.Context,
	cn *pbm.PBM,
	name,
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
			var ok bool
			ok, err := converged(ctx, cn, name, opid, shards, status)
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

func converged(ctx context.Context, cn *pbm.PBM, name, opid string, shards []topo.Shard, status defs.Status) (bool, error) {
	shardsToFinish := len(shards)
	bmeta, err := query.GetRestoreMeta(ctx, cn.Conn, name)
	if err != nil {
		return false, errors.Wrap(err, "get backup metadata")
	}

	clusterTime, err := topo.GetClusterTime(ctx, cn.Conn)
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	for _, sh := range shards {
		for _, shard := range bmeta.Replsets {
			if shard.Name == sh.RS {
				// check if node alive
				lck, err := lock.GetLockData(ctx, cn.Conn, &lock.LockHeader{
					Type:    defs.CmdRestore,
					OPID:    opid,
					Replset: shard.Name,
				})

				// nodes are cleaning its locks moving to the done status
				// so no lock is ok and not need to ckech the heartbeats
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
				case defs.StatusError:
					bmeta.Status = defs.StatusError
					bmeta.Error = shard.Error
					return false, errors.Errorf("restore on the shard %s failed with: %s", shard.Name, shard.Error)
				}
			}
		}
	}

	if shardsToFinish == 0 {
		err := query.ChangeRestoreState(ctx, cn.Conn, name, status, "")
		if err != nil {
			return false, errors.Wrapf(err, "update backup meta with %s", status)
		}
		return true, nil
	}

	return false, nil
}

func waitForStatus(ctx context.Context, cn *pbm.PBM, name string, status defs.Status) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			meta, err := query.GetRestoreMeta(ctx, cn.Conn, name)
			if errors.Is(err, errors.ErrNotFound) {
				continue
			}
			if err != nil {
				return errors.Wrap(err, "get restore metadata")
			}

			clusterTime, err := topo.GetClusterTime(ctx, cn.Conn)
			if err != nil {
				return errors.Wrap(err, "read cluster time")
			}

			if meta.Hb.T+defs.StaleFrameSec < clusterTime.T {
				return errors.Errorf("restore stuck, last beat ts: %d", meta.Hb.T)
			}

			switch meta.Status {
			case status:
				return nil
			case defs.StatusError:
				return errors.Errorf("cluster failed: %s", meta.Error)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// chunks defines chunks of oplog slice in given range, ensures its integrity (timeline
// is contiguous - there are no gaps), checks for respective files on storage and returns
// chunks list if all checks passed
func chunks(
	ctx context.Context,
	cn *pbm.PBM,
	stg storage.Storage,
	from,
	to primitive.Timestamp,
	rsName string,
	rsMap map[string]string,
) ([]oplog.OplogChunk, error) {
	mapRevRS := util.MakeReverseRSMapFunc(rsMap)
	chunks, err := oplog.PITRGetChunksSlice(ctx, cn.Conn, mapRevRS(rsName), from, to)
	if err != nil {
		return nil, errors.Wrap(err, "get chunks index")
	}

	if len(chunks) == 0 {
		return nil, errors.New("no chunks found")
	}

	if primitive.CompareTimestamp(chunks[len(chunks)-1].EndTS, to) == -1 {
		return nil, errors.Errorf(
			"no chunk with the target time, the last chunk ends on %v",
			chunks[len(chunks)-1].EndTS)
	}

	last := from
	for _, c := range chunks {
		if primitive.CompareTimestamp(last, c.StartTS) == -1 {
			return nil, errors.Errorf(
				"integrity vilolated, expect chunk with start_ts %v, but got %v",
				last, c.StartTS)
		}
		last = c.EndTS

		_, err := stg.FileStat(c.FName)
		if err != nil {
			return nil, errors.Errorf(
				"failed to ensure chunk %v.%v on the storage, file: %s, error: %v",
				c.StartTS, c.EndTS, c.FName, err)
		}
	}

	return chunks, nil
}

type applyOplogOption struct {
	start  *primitive.Timestamp
	end    *primitive.Timestamp
	nss    []string
	unsafe bool
	filter oplog.OpFilter
}

type (
	setcommittedTxnFn func(ctx context.Context, txn []types.RestoreTxn) error
	getcommittedTxnFn func(ctx context.Context) (map[string]primitive.Timestamp, error)
)

// By looking at just transactions in the oplog we can't tell which shards
// were participating in it. But we can assume that if there is
// commitTransaction at least on one shard then the transaction is committed
// everywhere. Otherwise, transactions won't be in the oplog or everywhere
// would be transactionAbort. So we just treat distributed as
// non-distributed - apply opps once a commit message for this txn is
// encountered.
// It might happen that by the end of the oplog there are some distributed txns
// without commit messages. We should commit such transactions only if the data is
// full (all prepared statements observed) and this txn was committed at least by
// one other shard. For that, each shard saves the last 100 dist transactions
// that were committed, so other shards can check if they should commit their
// leftovers. We store the last 100, as prepared statements and commits might be
// separated by other oplog events so it might happen that several commit messages
// can be cut away on some shards but present on other(s). Given oplog events of
// dist txns are more or less aligned in [cluster]time, checking the last 100
// should be more than enough.
// If the transaction is more than 16Mb it will be split into several prepared
// messages. So it might happen that one shard committed the txn but another has
// observed not all prepared messages by the end of the oplog. In such a case we
// should report it in logs and describe-restore.
//
//nolint:nonamedreturns
func applyOplog(
	ctx context.Context,
	node *mongo.Client,
	chunks []oplog.OplogChunk,
	options *applyOplogOption,
	sharded bool,
	ic *idx.IndexCatalog,
	setTxn setcommittedTxnFn,
	getTxn getcommittedTxnFn,
	stat *types.DistTxnStat,
	mgoV *version.MongoVersion,
	stg storage.Storage,
	log *log.Event,
) (partial []oplog.Txn, err error) {
	log.Info("starting oplog replay")

	var (
		ctxn       chan types.RestoreTxn
		txnSyncErr chan error
	)

	oplogRestore, err := oplog.NewOplogRestore(node, ic, mgoV, options.unsafe, true, ctxn, txnSyncErr)
	if err != nil {
		return nil, errors.Wrap(err, "create oplog")
	}

	oplogRestore.SetOpFilter(options.filter)

	var startTS, endTS primitive.Timestamp
	if options.start != nil {
		startTS = *options.start
	}
	if options.end != nil {
		endTS = *options.end
	}
	oplogRestore.SetTimeframe(startTS, endTS)
	oplogRestore.SetIncludeNS(options.nss)

	var lts primitive.Timestamp
	for _, chnk := range chunks {
		log.Debug("+ applying %v", chnk)

		// If the compression is Snappy and it failed we try S2.
		// Up until v1.7.0 the compression of pitr chunks was always S2.
		// But it was a mess in the code which lead to saving pitr chunk files
		// with the `.snappy`` extension although it was S2 in fact. And during
		// the restore, decompression treated .snappy as S2 ¯\_(ツ)_/¯ It wasn’t
		// an issue since there was no choice. Now, Snappy produces `.snappy` files
		// and S2 - `.s2` which is ok. But this means the old chunks (made by previous
		// PBM versions) won’t be compatible - during the restore, PBM will treat such
		// files as Snappy (judging by its suffix) but in fact, they are s2 files
		// and restore will fail with snappy: corrupt input. So we try S2 in such a case.
		lts, err = replayChunk(chnk.FName, oplogRestore, stg, chnk.Compression)
		if err != nil && errors.Is(err, snappy.ErrCorrupt) {
			lts, err = replayChunk(chnk.FName, oplogRestore, stg, defs.CompressionTypeS2)
		}
		if err != nil {
			return nil, errors.Wrapf(err, "replay chunk %v.%v", chnk.StartTS.T, chnk.EndTS.T)
		}
	}

	// dealing with dist txns
	if sharded {
		uc, c := oplogRestore.TxnLeftovers()
		stat.ShardUncommitted = len(uc)
		go func() {
			err := setTxn(ctx, c)
			if err != nil {
				log.Error("write last committed txns %v", err)
			}
		}()
		if len(uc) > 0 {
			commits, err := getTxn(ctx)
			if err != nil {
				return nil, errors.Wrap(err, "get committed txns on other shards")
			}
			var uncomm []oplog.Txn
			partial, uncomm, err = oplogRestore.HandleUncommittedTxn(commits)
			if err != nil {
				return nil, errors.Wrap(err, "handle ucommitted transactions")
			}
			if len(uncomm) > 0 {
				log.Info("uncommitted txns %d", len(uncomm))
			}
			stat.Partial = len(partial)
			stat.LeftUncommitted = len(uncomm)
		}
	}
	log.Info("oplog replay finished on %v", lts)

	return partial, nil
}

func replayChunk(
	file string,
	oplog *oplog.OplogRestore,
	stg storage.Storage,
	c defs.CompressionType,
) (primitive.Timestamp, error) {
	or, err := stg.SourceReader(file)
	if err != nil {
		lts := primitive.Timestamp{}
		return lts, errors.Wrapf(err, "get object %s form the storage", file)
	}
	defer or.Close()

	oplogReader, err := compress.Decompress(or, c)
	if err != nil {
		lts := primitive.Timestamp{}
		return lts, errors.Wrapf(err, "decompress object %s", file)
	}
	defer oplogReader.Close()

	lts, err := oplog.Apply(oplogReader)
	return lts, errors.Wrap(err, "apply oplog for chunk")
}
