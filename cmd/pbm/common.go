package main

import (
	"context"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

var errWaitTimeout = errors.New("Operation is in progress. Check pbm status and logs")

func sendCmd(ctx context.Context, conn connect.Client, cmd ctrl.Cmd) error {
	cmd.TS = time.Now().UTC().Unix()
	_, err := conn.CmdStreamCollection().InsertOne(ctx, cmd)
	return err
}
