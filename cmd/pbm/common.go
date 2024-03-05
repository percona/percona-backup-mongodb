package main

import (
	"context"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
)

func sendCmd(ctx context.Context, conn connect.Client, cmd ctrl.Cmd) error {
	cmd.TS = time.Now().UTC().Unix()
	_, err := conn.CmdStreamCollection().InsertOne(ctx, cmd)
	return err
}
