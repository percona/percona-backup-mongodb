package agent

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/pitr"
	"github.com/percona/percona-backup-mongodb/pbm/restore"
)

// OplogReplay replays oplog between r.Start and r.End timestamps (wall time in UTC tz)
func (a *Agent) OplogReplay(r *pbm.ReplayCmd, opID pbm.OPID, ep pbm.Epoch) {
	if r == nil {
		l := a.log.NewEvent(string(pbm.CmdReplay), "", opID.String(), ep.TS())
		l.Error("missed command")
		return
	}

	l := a.log.NewEvent(string(pbm.CmdReplay), r.Name, opID.String(), ep.TS())

	l.Info("time: %s-%s",
		time.Unix(int64(r.Start.T), 0).UTC().Format(time.RFC3339),
		time.Unix(int64(r.End.T), 0).UTC().Format(time.RFC3339),
	)

	nodeInfo, err := a.node.GetInfo()
	if err != nil {
		l.Error("get node info: %s", err.Error())
		return
	}
	if !nodeInfo.IsPrimary {
		l.Info("node in not suitable for restore")
		return
	}

	epoch := ep.TS()
	lock := a.pbm.NewLock(pbm.LockHeader{
		Type:    pbm.CmdReplay,
		Replset: nodeInfo.SetName,
		Node:    nodeInfo.Me,
		OPID:    opID.String(),
		Epoch:   &epoch,
	})

	nominated, err := a.acquireLock(lock, l, nil)
	if err != nil {
		l.Error("acquiring lock: %s", err.Error())
		return
	}
	if !nominated {
		l.Debug("oplog replay: skip: lock not acquired")
		return
	}

	defer func() {
		if err := lock.Release(); err != nil {
			l.Error("release lock: %s", err.Error())
		}
	}()

	l.Info("oplog replay started")
	if err := restore.New(a.pbm, a.node, r.RSMap).ReplayOplog(r, opID, l); err != nil {
		if errors.Is(err, restore.ErrNoDataForShard) {
			l.Info("no oplog for the shard, skipping")
		} else {
			l.Error("oplog replay: %v", err.Error())
		}
		return
	}
	l.Info("oplog replay successfully finished")

	resetEpoch, err := a.pbm.ResetEpoch()
	if err != nil {
		l.Error("reset epoch: %s", err.Error())
		return
	}

	l.Debug("epoch set to %v", resetEpoch)
}

func (a *Agent) EnsureOplog(r *pbm.EnsureOplogCmd, opID pbm.OPID, ep pbm.Epoch) {
	if r == nil {
		l := a.log.NewEvent(string(pbm.CmdEnsureOplog), "", opID.String(), ep.TS())
		l.Error("missed command")
		return
	}

	l := a.log.NewEvent(string(pbm.CmdEnsureOplog),
		fmt.Sprintf("%s-%s",
			time.Unix(int64(r.From.T), 0).UTC().Format(time.RFC3339),
			time.Unix(int64(r.Till.T), 0).UTC().Format(time.RFC3339)),
		opID.String(),
		ep.TS())

	nodeInfo, err := a.node.GetInfo()
	if err != nil {
		l.Error("get node info: %v", err)
		return
	}
	if nodeInfo.IsStandalone() {
		l.Error("cannot ensure oplog in standalone mode")
		return
	}

	from, err := findPreviousOplogTS(a.pbm.Context(), a.node.Session(), r.From)
	if err != nil {
		l.Error("lookup first oplog record: %s", err.Error())
		return
	}

	till, err := findFollowingOplogTS(a.pbm.Context(), a.node.Session(), r.Till)
	if err != nil {
		l.Error("lookup last oplog record: %s", err.Error())
		return
	}

	chunks, err := a.pbm.PITRGetChunksSlice(nodeInfo.SetName, from, till)
	if err != nil {
		l.Error("get chunks: %s", err.Error())
		return
	}

	missedChunks := findChunkRanges(chunks, from, till)
	if len(missedChunks) == 0 {
		l.Info("no missed oplog chunk")
		return
	}

	cfg, err := a.pbm.GetConfig()
	if err != nil {
		l.Error("get config: %s", err.Error())
		return
	}
	stg, err := a.pbm.GetStorage(l)
	if err != nil {
		l.Error("get storage: %s", err.Error())
		return
	}

	rsName := nodeInfo.SetName
	compression := compress.CompressionType(cfg.PITR.Compression)
	compressionLevel := cfg.PITR.CompressionLevel

	for _, t := range missedChunks {
		filename := pitr.ChunkName(rsName, t.from, t.till, compression)
		o := oplog.NewOplogBackup(a.node)
		o.SetTailingSpan(t.from, t.till)

		_, err = backup.Upload(a.pbm.Context(), o, stg, compression, compressionLevel, filename, -1)
		if err != nil {
			l.Error("unable to upload chunk %v.%v", t.from, t.till)
			l.Debug("remove %s due to upload errors", filename)

			if err := stg.Delete(filename); err != nil {
				l.Error("remove %s: %v", filename, err)
			}

			return
		}

		meta := pbm.OplogChunk{
			RS:          rsName,
			FName:       filename,
			Compression: compression,
			StartTS:     t.from,
			EndTS:       t.till,
		}

		if err := a.pbm.PITRAddChunk(meta); err != nil {
			l.Error("unable to save chunk meta %v: %s", meta, err.Error())
			return
		}

		l.Info("saved oplog chunk %s - %s", t.from, t.till)
	}

	l.Info("ensure oplog chunks: completed")
}

func findPreviousOplogTS(ctx context.Context, m *mongo.Client, ts primitive.Timestamp) (primitive.Timestamp, error) {
	res := m.Database("local").Collection("oplog.rs").FindOne(ctx,
		bson.M{"ts": bson.M{"$lte": ts}},
		options.FindOne().SetSort(bson.D{{"ts", -1}}))
	if err := res.Err(); err != nil {
		return primitive.Timestamp{}, err
	}

	var v struct{ TS primitive.Timestamp }
	if err := res.Decode(&v); err != nil {
		return primitive.Timestamp{}, errors.WithMessage(err, "decode")
	}

	return v.TS, nil
}

func findFollowingOplogTS(ctx context.Context, m *mongo.Client, ts primitive.Timestamp) (primitive.Timestamp, error) {
	res := m.Database("local").Collection("oplog.rs").FindOne(ctx,
		bson.M{"ts": bson.M{"$gte": ts}},
		options.FindOne().SetSort(bson.D{{"ts", 1}}))
	if err := res.Err(); err != nil {
		return primitive.Timestamp{}, err
	}

	var v struct{ TS primitive.Timestamp }
	if err := res.Decode(&v); err != nil {
		return primitive.Timestamp{}, errors.WithMessage(err, "decode")
	}

	return v.TS, nil
}

type timerange struct {
	from, till primitive.Timestamp
}

func findChunkRanges(rs []pbm.OplogChunk, from, till primitive.Timestamp) []timerange {
	if len(rs) == 0 {
		return []timerange{{from, till}}
	}

	rv := []timerange{}

	c := rs[0]
	if primitive.CompareTimestamp(from, c.StartTS) == -1 {
		rv = append(rv, timerange{from, c.StartTS})
	}

	endTS := c.EndTS
	for _, c = range rs[1:] {
		if primitive.CompareTimestamp(endTS, c.StartTS) == -1 {
			rv = append(rv, timerange{endTS, c.StartTS})
		}
		if primitive.CompareTimestamp(till, c.EndTS) != 1 {
			return rv
		}

		endTS = c.EndTS
	}

	if primitive.CompareTimestamp(endTS, till) == -1 {
		rv = append(rv, timerange{endTS, till})
	}

	return rv
}
