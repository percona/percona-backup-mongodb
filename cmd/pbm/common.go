package main

import (
	"time"

	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/types"
)

func sendCmd(ctx context.Context, m connect.Client, cmd types.Cmd) error {
	cmd.TS = time.Now().UTC().Unix()
	_, err := m.CmdStreamCollection().InsertOne(ctx, cmd)
	return err
}
