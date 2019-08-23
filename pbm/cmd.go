package pbm

import (
	"context"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (p *PBM) ListenCmd() (<-chan Cmd, <-chan error, error) {
	cmd := make(chan Cmd)
	errc := make(chan error)

	ctx := context.Background()
	var pipeline mongo.Pipeline
	cur, err := p.cmdC.Watch(ctx, pipeline, options.ChangeStream().SetFullDocument(options.UpdateLookup))
	if err != nil {
		return nil, nil, errors.Wrap(err, "watch the cmd stream")
	}

	go func() {
		defer close(cmd)
		defer close(errc)
		defer cur.Close(ctx)

		for cur.Next(ctx) {
			icmd := struct {
				C Cmd `bson:"fullDocument"`
			}{}

			err := cur.Decode(&icmd)
			if err != nil {
				errc <- errors.Wrap(err, "message decode")
				continue
			}

			cmd <- icmd.C
		}
		if cur.Err() != nil {
			errc <- errors.Wrap(cur.Err(), "cursor")
		}
	}()

	return cmd, errc, nil
}

func (p *PBM) SendCmd(cmd Cmd) error {
	_, err := p.cmdC.InsertOne(context.Background(), cmd)
	return err
}
