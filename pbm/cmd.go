package pbm

import (
	"fmt"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ErrorCursor struct {
	cerr error
}

func (c ErrorCursor) Error() string {
	return fmt.Sprintln("cursor was closed with:", c.cerr)
}

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
			errc <- ErrorCursor{cerr: cur.Err()}
			return
		}
	}()

	return cmd, errc, nil
}

func (p *PBM) SendCmd(cmd Cmd) error {
	_, err := p.Conn.Database(CmdStreamDB).Collection(CmdStreamCollection).InsertOne(p.ctx, cmd)
	return err
}
