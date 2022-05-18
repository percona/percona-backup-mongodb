package pbm

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type ErrorCursor struct {
	cerr error
}

func (c ErrorCursor) Error() string {
	return fmt.Sprintln("cursor was closed with:", c.cerr)
}

func (p *PBM) ListenCmd(cl <-chan struct{}) (<-chan Cmd, <-chan error) {
	cmd := make(chan Cmd)
	errc := make(chan error)

	go func() {
		defer close(cmd)
		defer close(errc)

		ts := time.Now().UTC().Add(0 * time.Hour).Unix()
		var lastTs int64
		var lastCmd Command
		for {
			select {
			case <-cl:
				return
			default:
			}
			cur, err := p.Conn.Database(DB).Collection(CmdStreamCollection).Find(
				p.ctx,
				bson.M{"ts": bson.M{"$gte": ts}},
			)
			if err != nil {
				errc <- errors.Wrap(err, "watch the cmd stream")
				continue
			}

			for cur.Next(p.ctx) {
				c := Cmd{}
				err := cur.Decode(&c)
				if err != nil {
					errc <- errors.Wrap(err, "message decode")
					continue
				}

				if c.Cmd == lastCmd && c.TS == lastTs {
					continue
				}

				opid, ok := cur.Current.Lookup("_id").ObjectIDOK()
				if !ok {
					errc <- errors.New("unable to get operation ID")
					continue
				}

				c.OPID = OPID(opid)

				lastCmd = c.Cmd
				lastTs = c.TS
				cmd <- c
				ts = time.Now().UTC().Unix()
			}
			if cur.Err() != nil {
				errc <- ErrorCursor{cerr: cur.Err()}
				cur.Close(p.ctx)
				return
			}
			cur.Close(p.ctx)
			time.Sleep(time.Second * 1)
		}
	}()

	return cmd, errc
}

func (p *PBM) SendCmd(cmd Cmd) error {
	cmd.TS = time.Now().UTC().Unix()
	_, err := p.Conn.Database(DB).Collection(CmdStreamCollection).InsertOne(p.ctx, cmd)
	return err
}

func UpdateCmdStatus(ctx context.Context, m *mongo.Client, cmd *Cmd, s Status) error {
	coll := m.Database(DB).Collection(CmdStreamCollection)

	cond := StatusCondition{
		Status: s,
		TS:     time.Now().Unix(),
	}
	_, err := coll.UpdateOne(ctx,
		bson.D{{"_id", primitive.ObjectID(cmd.OPID)}},
		bson.D{{"$push", bson.D{{"status.conditions", cond}}}})
	return err
}
