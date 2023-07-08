package pbm

import (
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
)

type CursorClosedError struct {
	Err error
}

func (c CursorClosedError) Error() string {
	return "cursor was closed with:" + c.Err.Error()
}

func (CursorClosedError) Is(err error) bool {
	if err == nil {
		return false
	}

	_, ok := err.(CursorClosedError) //nolint:errorlint
	return ok
}

func (c CursorClosedError) Unwrap() error {
	return c.Err
}

func (p *PBM) ListenCmd(cl <-chan struct{}) (<-chan Cmd, <-chan error) {
	cmd := make(chan Cmd)
	errc := make(chan error)

	go func() {
		defer close(cmd)
		defer close(errc)

		ts := time.Now().UTC().Unix()
		var lastTS int64
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

				if c.Cmd == lastCmd && c.TS == lastTS {
					continue
				}

				opid, ok := cur.Current.Lookup("_id").ObjectIDOK()
				if !ok {
					errc <- errors.New("unable to get operation ID")
					continue
				}

				c.OPID = OPID(opid)

				lastCmd = c.Cmd
				lastTS = c.TS
				cmd <- c
				ts = time.Now().UTC().Unix()
			}
			if err := cur.Err(); err != nil {
				errc <- CursorClosedError{err}
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
