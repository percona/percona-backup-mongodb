package client

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func getConfig(ctx context.Context, m *mongo.Client) (*pbm.Config, error) {
	res := coll(m, pbm.ConfigCollection).FindOne(ctx, bson.M{})
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrNotFound
		}
		return nil, errors.WithMessage(err, "query")
	}

	cfg := new(pbm.Config)
	err := res.Decode(&cfg)
	return cfg, errors.WithMessage(err, "decode")
}

type ConfigOptions struct {
	Deadline time.Time
}

func setConfig(ctx context.Context, m *mongo.Client, config *pbm.Config, opts *ConfigOptions) (primitive.ObjectID, error) {
	cmd := newCmd(pbm.CmdApplyConfig)
	cmd.Config = config

	if opts != nil && !opts.Deadline.IsZero() {
		cmd.Context = &pbm.CmdContext{
			Deadline: opts.Deadline,
		}
	}

	id, err := sendCommand(ctx, m, cmd)
	return id, errors.WithMessage(err, "send command")
}
