package pbm

import (
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (p *PBM) ListenCmd() (<-chan Cmd, <-chan error, error) {
	cmd := make(chan Cmd)
	errc := make(chan error)

	var pipeline mongo.Pipeline
	cur, err := p.Conn.Database(CmdStreamDB).Collection(CmdStreamCollection).Watch(p.ctx, pipeline, options.ChangeStream().SetFullDocument(options.UpdateLookup))
	if err != nil {
		return nil, nil, errors.Wrap(err, "watch the cmd stream")
	}

	go func() {
		defer close(cmd)
		defer close(errc)
		defer cur.Close(p.ctx)

		for cur.Next(p.ctx) {
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
	_, err := p.Conn.Database(CmdStreamDB).Collection(CmdStreamCollection).InsertOne(p.ctx, cmd)
	return err
}
