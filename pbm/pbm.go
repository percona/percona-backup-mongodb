package pbm

import (
	"context"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// DB is a name of the PBM database
	DB = "admin"
	// LogCollection is the name of the mongo collection that contains PBM logs
	LogCollection = "pbmLog"
	// ConfigCollection is the name of the mongo collection that contains PBM configs
	ConfigCollection = "pbmConfig"

	// CmdStreamDB is the cmd database
	CmdStreamDB = "pbm"
	// CmdStreamCollection is the name of the mongo collection that contains backup/restore commands stream
	CmdStreamCollection = "cmd"
)

type Command string

const (
	CmdUndefined Command = ""
	CmdBackup            = "backup"
	CmdRestore           = "restore"
)

type Cmd struct {
	Cmd  Command `bson:"cmd"`
	Name string  `bson:"name"`
}

type PBM struct {
	Conn    *mongo.Client
	configC *mongo.Collection
	logsC   *mongo.Collection
	cmdC    *mongo.Collection
}

func New(pbmConn *mongo.Client) *PBM {
	return &PBM{
		Conn:    pbmConn,
		configC: pbmConn.Database(DB).Collection(ConfigCollection),
		logsC:   pbmConn.Database(DB).Collection(LogCollection),
		cmdC:    pbmConn.Database(CmdStreamDB).Collection(CmdStreamCollection),
	}
}

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
