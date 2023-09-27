package query

import (
	"time"

	"github.com/mongodb/mongo-tools/common/db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/topo"
	"github.com/percona/percona-backup-mongodb/internal/types"
)

func GetRestoreMetaByOPID(ctx context.Context, m connect.MetaClient, opid string) (*types.RestoreMeta, error) {
	return getRestoreMeta(ctx, m, bson.D{{"opid", opid}})
}

func GetRestoreMeta(ctx context.Context, m connect.MetaClient, name string) (*types.RestoreMeta, error) {
	return getRestoreMeta(ctx, m, bson.D{{"name", name}})
}

func getRestoreMeta(ctx context.Context, m connect.MetaClient, clause bson.D) (*types.RestoreMeta, error) {
	res := m.RestoresCollection().FindOne(ctx, clause)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errors.ErrNotFound
		}
		return nil, errors.Wrap(err, "get")
	}
	r := &types.RestoreMeta{}
	err := res.Decode(r)
	return r, errors.Wrap(err, "decode")
}

func ChangeRestoreStateOPID(ctx context.Context, m connect.MetaClient, opid string, s defs.Status, msg string) error {
	return changeRestoreState(ctx, m, bson.D{{"name", opid}}, s, msg)
}

func ChangeRestoreState(ctx context.Context, m connect.MetaClient, name string, s defs.Status, msg string) error {
	return changeRestoreState(ctx, m, bson.D{{"name", name}}, s, msg)
}

func changeRestoreState(ctx context.Context, m connect.MetaClient, clause bson.D, s defs.Status, msg string) error {
	ts := time.Now().UTC().Unix()
	_, err := m.RestoresCollection().UpdateOne(
		ctx,
		clause,
		bson.D{
			{"$set", bson.M{"status": s}},
			{"$set", bson.M{"last_transition_ts": ts}},
			{"$set", bson.M{"error": msg}},
			{"$push", bson.M{"conditions": types.Condition{Timestamp: ts, Status: s, Error: msg}}},
		},
	)

	return err
}

func ChangeRestoreRSState(
	ctx context.Context,
	m connect.MetaClient,
	name,
	rsName string,
	s defs.Status,
	msg string,
) error {
	ts := time.Now().UTC().Unix()
	_, err := m.RestoresCollection().UpdateOne(
		ctx,
		bson.D{{"name", name}, {"replsets.name", rsName}},
		bson.D{
			{"$set", bson.M{"replsets.$.status": s}},
			{"$set", bson.M{"replsets.$.last_transition_ts": ts}},
			{"$set", bson.M{"replsets.$.error": msg}},
			{"$push", bson.M{"replsets.$.conditions": types.Condition{Timestamp: ts, Status: s, Error: msg}}},
		},
	)

	return err
}

func RestoreSetRSTxn(
	ctx context.Context,
	m connect.MetaClient,
	name, rsName string,
	txn []types.RestoreTxn,
) error {
	_, err := m.RestoresCollection().UpdateOne(
		ctx,
		bson.D{{"name", name}, {"replsets.name", rsName}},
		bson.D{{"$set", bson.M{"replsets.$.committed_txn": txn, "replsets.$.txn_set": true}}},
	)

	return err
}

func RestoreSetRSStat(
	ctx context.Context,
	m connect.MetaClient,
	name, rsName string,
	stat types.RestoreShardStat,
) error {
	_, err := m.RestoresCollection().UpdateOne(
		ctx,
		bson.D{{"name", name}, {"replsets.name", rsName}},
		bson.D{{"$set", bson.M{"replsets.$.stat": stat}}},
	)

	return err
}

func RestoreSetStat(ctx context.Context, m connect.MetaClient, name string, stat types.RestoreStat) error {
	_, err := m.RestoresCollection().UpdateOne(
		ctx,
		bson.D{{"name", name}},
		bson.D{{"$set", bson.M{"stat": stat}}},
	)

	return err
}

func RestoreSetRSPartTxn(ctx context.Context, m connect.MetaClient, name, rsName string, txn []db.Oplog) error {
	_, err := m.RestoresCollection().UpdateOne(
		ctx,
		bson.D{{"name", name}, {"replsets.name", rsName}},
		bson.D{{"$set", bson.M{"replsets.$.partial_txn": txn}}},
	)

	return err
}

func SetCurrentOp(ctx context.Context, m connect.MetaClient, name, rsName string, ts primitive.Timestamp) error {
	_, err := m.RestoresCollection().UpdateOne(
		ctx,
		bson.D{{"name", name}, {"replsets.name", rsName}},
		bson.D{{"$set", bson.M{"replsets.$.op": ts}}},
	)

	return err
}

func SetRestoreMeta(ctx context.Context, m connect.MetaClient, meta *types.RestoreMeta) error {
	meta.LastTransitionTS = meta.StartTS
	meta.Conditions = append(meta.Conditions, &types.Condition{
		Timestamp: meta.StartTS,
		Status:    meta.Status,
	})

	_, err := m.RestoresCollection().InsertOne(ctx, m)

	return err
}

// GetLastRestore returns last successfully finished restore
// and nil if there is no such restore yet.
func GetLastRestore(ctx context.Context, m connect.MetaClient) (*types.RestoreMeta, error) {
	r := &types.RestoreMeta{}

	res := m.RestoresCollection().FindOne(
		ctx,
		bson.D{{"status", defs.StatusDone}},
		options.FindOne().SetSort(bson.D{{"start_ts", -1}}),
	)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errors.ErrNotFound
		}
		return nil, errors.Wrap(err, "get")
	}
	err := res.Decode(r)
	return r, errors.Wrap(err, "decode")
}

func AddRestoreRSMeta(ctx context.Context, m connect.MetaClient, name string, rs types.RestoreReplset) error {
	rs.LastTransitionTS = rs.StartTS
	rs.Conditions = append(rs.Conditions, &types.Condition{
		Timestamp: rs.StartTS,
		Status:    rs.Status,
	})
	_, err := m.RestoresCollection().UpdateOne(
		ctx,
		bson.D{{"name", name}},
		bson.D{{"$addToSet", bson.M{"replsets": rs}}},
	)

	return err
}

func RestoreHB(ctx context.Context, m connect.MetaClient, name string) error {
	ts, err := topo.GetClusterTime(ctx, m)
	if err != nil {
		return errors.Wrap(err, "read cluster time")
	}

	_, err = m.RestoresCollection().UpdateOne(
		ctx,
		bson.D{{"name", name}},
		bson.D{
			{"$set", bson.M{"hb": ts}},
		},
	)

	return errors.Wrap(err, "write into db")
}

func SetRestoreBackup(ctx context.Context, m connect.MetaClient, name, backupName string, nss []string) error {
	d := bson.M{"backup": backupName}
	if nss != nil {
		d["nss"] = nss
	}

	_, err := m.RestoresCollection().UpdateOne(
		ctx,
		bson.D{{"name", name}},
		bson.D{{"$set", d}},
	)

	return err
}

func SetOplogTimestamps(ctx context.Context, m connect.MetaClient, name string, start, end int64) error {
	_, err := m.RestoresCollection().UpdateOne(
		ctx,
		bson.M{"name": name},
		bson.M{"$set": bson.M{"start_pitr": start, "pitr": end}},
	)

	return err
}

func RestoresList(ctx context.Context, m connect.MetaClient, limit int64) ([]types.RestoreMeta, error) {
	cur, err := m.RestoresCollection().Find(
		ctx,
		bson.M{},
		options.Find().SetLimit(limit).SetSort(bson.D{{"start_ts", -1}}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "query mongo")
	}
	defer cur.Close(ctx)

	restores := []types.RestoreMeta{}
	for cur.Next(ctx) {
		r := types.RestoreMeta{}
		err := cur.Decode(&r)
		if err != nil {
			return nil, errors.Wrap(err, "message decode")
		}
		restores = append(restores, r)
	}

	return restores, cur.Err()
}
