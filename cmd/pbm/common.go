package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/sdk"
)

var errWaitTimeout = errors.New("Operation is in progress. Check pbm status and logs")

var errUserCanceled = errors.New("canceled")

func askConfirmation(question string) error {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return errors.Wrap(err, "stat stdin")
	}
	if (fi.Mode() & os.ModeCharDevice) == 0 {
		return errors.New("no tty")
	}

	fmt.Printf("%s [y/N] ", question)

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	if err := scanner.Err(); err != nil {
		return errors.Wrap(err, "read stdin")
	}

	switch strings.TrimSpace(scanner.Text()) {
	case "yes", "Yes", "YES", "Y", "y":
		return nil
	}

	return errUserCanceled
}

func sendCmd(ctx context.Context, conn connect.Client, cmd ctrl.Cmd) error {
	cmd.TS = time.Now().UTC().Unix()
	_, err := conn.CmdStreamCollection().InsertOne(ctx, cmd)
	return err
}

func checkForAnotherOperation(ctx context.Context, pbm *sdk.Client) error {
	locks, err := pbm.OpLocks(ctx)
	if err != nil {
		return errors.Wrap(err, "get operation lock")
	}
	if len(locks) == 0 {
		return nil
	}

	ts, err := sdk.ClusterTime(ctx, pbm)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}

	for _, l := range locks {
		if l.Heartbeat.T+defs.StaleFrameSec >= ts.T {
			return &concurrentOpError{l}
		}
	}

	return nil
}

func waitForResyncWithTimeout(ctx context.Context, pbm *sdk.Client, cid sdk.CommandID, timeout time.Duration) error {
	if timeout > time.Second {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if err := sdk.WaitForResync(ctx, pbm, cid); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			err = errWaitTimeout
		}

		return errors.Wrapf(err, "waiting for resync [opid %q]", cid)
	}
	return nil
}

type concurrentOpError struct{ sdk.OpLock }

func (e *concurrentOpError) Error() string {
	return fmt.Sprintf("another operation in progress, %s/%s [%s/%s]",
		e.Cmd, e.OpID, e.Replset, e.Node)
}

func (e *concurrentOpError) MarshalJSON() ([]byte, error) {
	s := map[string]any{
		"error": "another operation in progress",
		"operation": map[string]any{
			"type":    e.Cmd,
			"opid":    e.OpID,
			"replset": e.Replset,
			"node":    e.Node,
		},
	}

	return json.Marshal(s)
}
