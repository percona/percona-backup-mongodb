package oplog

import (
	"context"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// PITRMeta contains all operational data about PITR execution process.
type PITRMeta struct {
	StartTS int64 `bson:"start_ts" json:"start_ts"`
	// Hb         primitive.Timestamp `bson:"hb" json:"hb"`
	// Status     defs.Status         `bson:"status" json:"status"`
	Nomination []PITRNomination `bson:"n" json:"n"`
}

// PITRNomination is used to choose (nominate and elect) member(s)
// which will perform PITR process within a replica set(s).
type PITRNomination struct {
	RS    string   `bson:"rs" json:"rs"`
	Nodes []string `bson:"n" json:"n"`
	Ack   string   `bson:"ack" json:"ack"`
}

// Init add initial PITR document.
func InitMeta(ctx context.Context, conn connect.Client) error {
	pitrMeta := PITRMeta{
		StartTS: time.Now().Unix(),
	}
	_, err := conn.PITRCollection().ReplaceOne(
		ctx,
		bson.D{},
		pitrMeta,
		options.Replace().SetUpsert(true),
	)

	return errors.Wrap(err, "pitr meta replace")
}

// SetPITRNomination adds nomination fragment for specified RS within PITRMeta.
func SetPITRNomination(ctx context.Context, conn connect.Client, rs string) error {
	n := PITRNomination{
		RS:    rs,
		Nodes: []string{},
	}
	_, err := conn.PITRCollection().
		UpdateOne(
			ctx,
			bson.D{},
			bson.D{{"$addToSet", bson.M{"n": n}}},
			options.Update().SetUpsert(true),
		)

	return errors.Wrap(err, "update pitr nomination")
}

// GetPITRNominees fetches nomination fragment for specified RS
// from PITRMeta document.
func GetPITRNominees(
	ctx context.Context,
	conn connect.Client,
	rs string,
) (*PITRNomination, error) {
	res := conn.PITRCollection().FindOne(ctx, bson.D{})
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errors.ErrNotFound
		}
		return nil, errors.Wrap(err, "find pitr meta")
	}

	meta := &PITRMeta{}
	if err := res.Decode(meta); err != nil {
		errors.Wrap(err, "decode")
	}

	for _, n := range meta.Nomination {
		if n.RS == rs {
			return &n, nil
		}
	}

	return nil, errors.ErrNotFound
}

// SetPITRNominees add nominee(s) for specific RS.
// It is used by cluster leader within nomination process.
func SetPITRNominees(
	ctx context.Context,
	conn connect.Client,
	rs string,
	nodes []string,
) error {
	_, err := conn.PITRCollection().UpdateOne(
		ctx,
		bson.D{
			{"n.rs", rs},
		},
		bson.D{
			{"$addToSet", bson.M{"n.$.n": bson.M{"$each": nodes}}},
		},
	)

	return errors.Wrap(err, "update pitr nominees")
}

// SetPITRNomineeACK add ack for specific nomination.
// It is used by nominee, after the nomination is created by cluster leader.
func SetPITRNomineeACK(
	ctx context.Context,
	conn connect.Client,
	rs,
	node string,
) error {
	_, err := conn.PITRCollection().UpdateOne(
		ctx,
		bson.D{{"n.rs", rs}},
		bson.D{
			{"$set", bson.M{"n.$.ack": node}},
		},
	)

	return errors.Wrap(err, "update pitr nominee ack")
}
