package query

import (
	"time"

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

func GetBackupMeta(ctx context.Context, m connect.MetaClient, name string) (*types.BackupMeta, error) {
	return getBackupMeta(ctx, m, bson.D{{"name", name}})
}

func GetBackupByOPID(ctx context.Context, m connect.MetaClient, opid string) (*types.BackupMeta, error) {
	return getBackupMeta(ctx, m, bson.D{{"opid", opid}})
}

func getBackupMeta(ctx context.Context, m connect.MetaClient, clause bson.D) (*types.BackupMeta, error) {
	res := m.BcpCollection().FindOne(ctx, clause)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errors.ErrNotFound
		}
		return nil, errors.Wrap(err, "get")
	}

	b := &types.BackupMeta{}
	err := res.Decode(b)
	return b, errors.Wrap(err, "decode")
}

func ChangeBackupStateOPID(ctx context.Context, m connect.MetaClient, opid string, s defs.Status, msg string) error {
	return changeBackupState(ctx, m, bson.D{{"opid", opid}}, s, msg)
}

func ChangeBackupState(ctx context.Context, m connect.MetaClient, bcpName string, s defs.Status, msg string) error {
	return changeBackupState(ctx, m, bson.D{{"name", bcpName}}, s, msg)
}

func changeBackupState(ctx context.Context, m connect.MetaClient, clause bson.D, s defs.Status, msg string) error {
	ts := time.Now().UTC().Unix()
	_, err := m.BcpCollection().UpdateOne(
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

func SetBackupMeta(ctx context.Context, m connect.MetaClient, meta *types.BackupMeta) error {
	meta.LastTransitionTS = meta.StartTS
	meta.Conditions = append(meta.Conditions, types.Condition{
		Timestamp: meta.StartTS,
		Status:    meta.Status,
	})

	_, err := m.BcpCollection().InsertOne(ctx, m)

	return err
}

func BackupHB(ctx context.Context, m connect.MetaClient, bcpName string) error {
	ts, err := topo.GetClusterTime(ctx, m)
	if err != nil {
		return errors.Wrap(err, "read cluster time")
	}

	_, err = m.BcpCollection().UpdateOne(
		ctx,
		bson.D{{"name", bcpName}},
		bson.D{
			{"$set", bson.M{"hb": ts}},
		},
	)

	return errors.Wrap(err, "write into db")
}

func SetSrcBackup(ctx context.Context, m connect.MetaClient, bcpName, srcName string) error {
	_, err := m.BcpCollection().UpdateOne(
		ctx,
		bson.D{{"name", bcpName}},
		bson.D{
			{"$set", bson.M{"src_backup": srcName}},
		},
	)

	return err
}

func SetFirstWrite(ctx context.Context, m connect.MetaClient, bcpName string, first primitive.Timestamp) error {
	_, err := m.BcpCollection().UpdateOne(
		ctx,
		bson.D{{"name", bcpName}},
		bson.D{
			{"$set", bson.M{"first_write_ts": first}},
		},
	)

	return err
}

func SetLastWrite(ctx context.Context, m connect.MetaClient, bcpName string, last primitive.Timestamp) error {
	_, err := m.BcpCollection().UpdateOne(
		ctx,
		bson.D{{"name", bcpName}},
		bson.D{
			{"$set", bson.M{"last_write_ts": last}},
		},
	)

	return err
}

func AddRSMeta(ctx context.Context, m connect.MetaClient, bcpName string, rs types.BackupReplset) error {
	rs.LastTransitionTS = rs.StartTS
	rs.Conditions = append(rs.Conditions, types.Condition{
		Timestamp: rs.StartTS,
		Status:    rs.Status,
	})
	_, err := m.BcpCollection().UpdateOne(
		ctx,
		bson.D{{"name", bcpName}},
		bson.D{{"$addToSet", bson.M{"replsets": rs}}},
	)

	return err
}

func ChangeRSState(ctx context.Context, m connect.MetaClient, bcpName, rsName string, s defs.Status, msg string) error {
	ts := time.Now().UTC().Unix()
	_, err := m.BcpCollection().UpdateOne(
		ctx,
		bson.D{{"name", bcpName}, {"replsets.name", rsName}},
		bson.D{
			{"$set", bson.M{"replsets.$.status": s}},
			{"$set", bson.M{"replsets.$.last_transition_ts": ts}},
			{"$set", bson.M{"replsets.$.error": msg}},
			{"$push", bson.M{"replsets.$.conditions": types.Condition{Timestamp: ts, Status: s, Error: msg}}},
		},
	)

	return err
}

func IncBackupSize(ctx context.Context, m connect.MetaClient, bcpName string, size int64) error {
	_, err := m.BcpCollection().UpdateOne(ctx,
		bson.D{{"name", bcpName}},
		bson.D{{"$inc", bson.M{"size": size}}})

	return err
}

func RSSetPhyFiles(ctx context.Context, m connect.MetaClient, bcpName, rsName string, rs *types.BackupReplset) error {
	_, err := m.BcpCollection().UpdateOne(
		ctx,
		bson.D{{"name", bcpName}, {"replsets.name", rsName}},
		bson.D{
			{"$set", bson.M{"replsets.$.files": rs.Files}},
			{"$set", bson.M{"replsets.$.journal": rs.Journal}},
		},
	)

	return err
}

func SetRSLastWrite(ctx context.Context, m connect.MetaClient, bcpName, rsName string, ts primitive.Timestamp) error {
	_, err := m.BcpCollection().UpdateOne(
		ctx,
		bson.D{{"name", bcpName}, {"replsets.name", rsName}},
		bson.D{
			{"$set", bson.M{"replsets.$.last_write_ts": ts}},
		},
	)

	return err
}

func LastIncrementalBackup(ctx context.Context, m connect.MetaClient) (*types.BackupMeta, error) {
	return getRecentBackup(ctx, m, nil, nil, -1, bson.D{{"type", string(defs.IncrementalBackup)}})
}

// GetLastBackup returns last successfully finished backup (non-selective and non-external)
// or nil if there is no such backup yet. If ts isn't nil it will
// search for the most recent backup that finished before specified timestamp
func GetLastBackup(ctx context.Context, m connect.MetaClient, before *primitive.Timestamp) (*types.BackupMeta, error) {
	return getRecentBackup(ctx, m, nil, before, -1,
		bson.D{{"nss", nil}, {"type", bson.M{"$ne": defs.ExternalBackup}}})
}

func GetFirstBackup(ctx context.Context, m connect.MetaClient, after *primitive.Timestamp) (*types.BackupMeta, error) {
	return getRecentBackup(ctx, m, after, nil, 1,
		bson.D{{"nss", nil}, {"type", bson.M{"$ne": defs.ExternalBackup}}})
}

func getRecentBackup(
	ctx context.Context,
	m connect.MetaClient,
	after,
	before *primitive.Timestamp,
	sort int,
	opts bson.D,
) (*types.BackupMeta, error) {
	q := append(bson.D{}, opts...)
	q = append(q, bson.E{"status", defs.StatusDone})
	if after != nil {
		q = append(q, bson.E{"last_write_ts", bson.M{"$gte": after}})
	}
	if before != nil {
		q = append(q, bson.E{"last_write_ts", bson.M{"$lte": before}})
	}

	res := m.BcpCollection().FindOne(
		ctx,
		q,
		options.FindOne().SetSort(bson.D{{"start_ts", sort}}),
	)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errors.ErrNotFound
		}
		return nil, errors.Wrap(err, "get")
	}

	b := &types.BackupMeta{}
	err := res.Decode(b)
	return b, errors.Wrap(err, "decode")
}

func BackupHasNext(ctx context.Context, m connect.MetaClient, backup *types.BackupMeta) (bool, error) {
	f := bson.D{
		{"nss", nil},
		{"type", bson.M{"$ne": defs.ExternalBackup}},
		{"start_ts", bson.M{"$gt": backup.LastWriteTS.T}},
		{"status", defs.StatusDone},
	}
	o := options.FindOne().SetProjection(bson.D{{"_id", 1}})
	res := m.BcpCollection().FindOne(ctx, f, o)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return false, nil
		}
		return false, errors.Wrap(err, "query")
	}

	return true, nil
}

func BackupsList(ctx context.Context, m connect.MetaClient, limit int64) ([]types.BackupMeta, error) {
	cur, err := m.BcpCollection().Find(
		ctx,
		bson.M{},
		options.Find().SetLimit(limit).SetSort(bson.D{{"start_ts", -1}}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "query mongo")
	}
	defer cur.Close(ctx)

	backups := []types.BackupMeta{}
	for cur.Next(ctx) {
		b := types.BackupMeta{}
		err := cur.Decode(&b)
		if err != nil {
			return nil, errors.Wrap(err, "message decode")
		}
		if b.Type == "" {
			b.Type = defs.LogicalBackup
		}
		backups = append(backups, b)
	}

	return backups, cur.Err()
}

func BackupsDoneList(
	ctx context.Context,
	m connect.MetaClient,
	after *primitive.Timestamp,
	limit int64,
	order int,
) ([]types.BackupMeta, error) {
	q := bson.D{{"status", defs.StatusDone}}
	if after != nil {
		q = append(q, bson.E{"last_write_ts", bson.M{"$gte": after}})
	}

	cur, err := m.BcpCollection().Find(
		ctx,
		q,
		options.Find().SetLimit(limit).SetSort(bson.D{{"last_write_ts", order}}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "query mongo")
	}
	defer cur.Close(ctx)

	backups := []types.BackupMeta{}
	for cur.Next(ctx) {
		b := types.BackupMeta{}
		err := cur.Decode(&b)
		if err != nil {
			return nil, errors.Wrap(err, "message decode")
		}
		backups = append(backups, b)
	}

	return backups, cur.Err()
}

func SetRSNomination(ctx context.Context, m connect.MetaClient, bcpName, rs string) error {
	n := types.BackupRsNomination{RS: rs, Nodes: []string{}}
	_, err := m.BcpCollection().
		UpdateOne(
			ctx,
			bson.D{{"name", bcpName}},
			bson.D{{"$addToSet", bson.M{"n": n}}},
		)

	return errors.Wrap(err, "query")
}

func GetRSNominees(ctx context.Context, m connect.MetaClient, bcpName, rsName string) (*types.BackupRsNomination, error) {
	bcp, err := GetBackupMeta(ctx, m, bcpName)
	if err != nil {
		return nil, err
	}

	for _, n := range bcp.Nomination {
		if n.RS == rsName {
			return &n, nil
		}
	}

	return nil, errors.ErrNotFound
}

func SetRSNominees(ctx context.Context, m connect.MetaClient, bcpName, rsName string, nodes []string) error {
	_, err := m.BcpCollection().UpdateOne(
		ctx,
		bson.D{{"name", bcpName}, {"n.rs", rsName}},
		bson.D{
			{"$set", bson.M{"n.$.n": nodes}},
		},
	)

	return err
}

func SetRSNomineeACK(ctx context.Context, m connect.MetaClient, bcpName, rsName, node string) error {
	_, err := m.BcpCollection().UpdateOne(
		ctx,
		bson.D{{"name", bcpName}, {"n.rs", rsName}},
		bson.D{
			{"$set", bson.M{"n.$.ack": node}},
		},
	)

	return err
}
