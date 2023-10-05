package ctrl

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/internal/connect"
)

func SendResync(ctx context.Context, m connect.Client) (OPID, error) {
	return sendCommand(ctx, m, Cmd{Cmd: CmdResync})
}

func SendCancelBackup(ctx context.Context, m connect.Client) (OPID, error) {
	return sendCommand(ctx, m, Cmd{Cmd: CmdCancelBackup})
}

func sendCommand(ctx context.Context, m connect.Client, cmd Cmd) (OPID, error) {
	cmd.TS = time.Now().UTC().Unix()
	res, err := m.CmdStreamCollection().InsertOne(ctx, cmd)
	if err != nil {
		return NilOPID(), err
	}

	return OPID(res.InsertedID.(primitive.ObjectID)), nil
}
