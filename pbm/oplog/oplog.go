package oplog

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

var errNoTransaction = errors.New("no transaction found")

func GetOplogStartTime(ctx context.Context, m *mongo.Client) (primitive.Timestamp, error) {
	ts, err := findTransactionStartTime(ctx, m)
	if errors.Is(err, errNoTransaction) {
		ts, err = findLastOplogTS(ctx, m)
	}

	return ts, err
}

func findTransactionStartTime(ctx context.Context, m *mongo.Client) (primitive.Timestamp, error) {
	coll := m.Database("config").Collection("transactions", options.Collection().SetReadConcern(readconcern.Local()))
	f := bson.D{{"state", bson.D{{"$in", bson.A{"prepared", "inProgress"}}}}}
	o := options.FindOne().SetSort(bson.D{{"startOpTime", 1}})
	doc, err := coll.FindOne(ctx, f, o).Raw()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return primitive.Timestamp{}, errNoTransaction
		}
		return primitive.Timestamp{}, errors.Wrap(err, "query transactions")
	}

	rawTS, err := doc.LookupErr("startOpTime", "ts")
	if err != nil {
		return primitive.Timestamp{}, errors.Wrap(err, "lookup timestamp")
	}

	t, i, ok := rawTS.TimestampOK()
	if !ok {
		return primitive.Timestamp{}, errors.Wrap(err, "parse timestamp")
	}

	return primitive.Timestamp{T: t, I: i}, nil
}

func findLastOplogTS(ctx context.Context, m *mongo.Client) (primitive.Timestamp, error) {
	coll := m.Database("local").Collection("oplog.rs")
	o := options.FindOne().SetSort(bson.M{"$natural": -1})
	doc, err := coll.FindOne(ctx, bson.D{}, o).Raw()
	if err != nil {
		return primitive.Timestamp{}, errors.Wrap(err, "query oplog")
	}

	rawTS, err := doc.LookupErr("ts")
	if err != nil {
		return primitive.Timestamp{}, errors.Wrap(err, "lookup oplog ts")
	}

	t, i, ok := rawTS.TimestampOK()
	if !ok {
		return primitive.Timestamp{}, errors.Wrap(err, "parse oplog ts")
	}

	return primitive.Timestamp{T: t, I: i}, nil
}
