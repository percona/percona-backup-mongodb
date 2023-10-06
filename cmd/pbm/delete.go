package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/internal/backup"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/oplog"
	"github.com/percona/percona-backup-mongodb/internal/util"
	"github.com/percona/percona-backup-mongodb/sdk"
)

type deleteBcpOpts struct {
	name      string
	olderThan string
	bcpType   string
	force     bool
}

func deleteBackup(
	ctx context.Context,
	pbm sdk.Client,
	d *deleteBcpOpts,
	outf outFormat,
) (fmt.Stringer, error) {
	if d.name != "" && d.olderThan != "" {
		return nil, errors.New("cannot use --name and --older-then at the same command")
	}
	if d.name == "" && d.olderThan == "" {
		return nil, errors.New("either --name or --older-than should be set")
	}

	if !d.force {
		if err := askConfirmation("Are you sure you want to delete backup(s)?"); err != nil {
			if errors.Is(err, errUserCanceled) {
				return outMsg{err.Error()}, nil
			}
			return nil, err
		}
	}

	var cid sdk.CommandID
	var err error
	if d.name != "" {
		cid, err = pbm.DeleteBackupByName(ctx, d.name)
	} else { // d.olderThan != ""
		var ts primitive.Timestamp
		ts, err = parseOlderThan(d.olderThan)
		if err != nil {
			return nil, errors.Wrap(err, "parse --older-than")
		}

		cid, err = pbm.DeleteBackupBefore(ctx, ts)
	}
	if err != nil {
		return nil, errors.Wrap(err, "schedule delete")
	}

	if outf != outText {
		return nil, nil
	}

	return waitForDelete(ctx, pbm, cid)
}

type deletePitrOpts struct {
	olderThan string
	force     bool
	all       bool
}

func deletePITR(
	ctx context.Context,
	pbm sdk.Client,
	d *deletePitrOpts,
	outf outFormat,
) (fmt.Stringer, error) {
	if d.olderThan != "" && d.all {
		return nil, errors.New("cannot use --older-then and --all at the same command")
	}
	if !d.all && d.olderThan == "" {
		return nil, errors.New("either --older-than or --all should be set")
	}

	if !d.force {
		q := "Are you sure you want to delete chunks?"
		if d.all {
			q = "Are you sure you want to delete ALL chunks?"
		}
		if err := askConfirmation(q); err != nil {
			if errors.Is(err, errUserCanceled) {
				return outMsg{err.Error()}, nil
			}
			return nil, err
		}
	}

	var ts primitive.Timestamp
	if d.olderThan != "" {
		var err error
		ts, err = parseOlderThan(d.olderThan)
		if err != nil {
			return nil, errors.Wrap(err, "parse --older-then")
		}
	}
	cid, err := pbm.DeleteOplogRange(ctx, ts)
	if err != nil {
		return nil, errors.Wrap(err, "schedule pitr delete")
	}

	if outf != outText {
		return nil, nil
	}

	return waitForDelete(ctx, pbm, cid)
}

type cleanupOptions struct {
	olderThan string
	yes       bool
	wait      bool
	dryRun    bool
}

func retentionCleanup(
	ctx context.Context,
	pbm sdk.Client,
	d *cleanupOptions,
) (fmt.Stringer, error) {
	ts, err := parseOlderThan(d.olderThan)
	if err != nil {
		return nil, errors.Wrap(err, "parse --older-than")
	}
	info, err := pbm.CleanupReport(ctx, ts)
	if err != nil {
		return nil, errors.Wrap(err, "make cleanup report")
	}
	if len(info.Backups) == 0 && len(info.Chunks) == 0 {
		return outMsg{"nothing to delete"}, nil
	}

	if d.dryRun {
		b := &strings.Builder{}
		printCleanupInfoTo(b, info.Backups, info.Chunks)
		return b, nil
	}

	if !d.yes {
		if err := askCleanupConfirmation(info); err != nil {
			if errors.Is(err, errUserCanceled) {
				return outMsg{err.Error()}, nil
			}
			return nil, err
		}
	}

	cid, err := pbm.RunCleanup(ctx, ts)
	if err != nil {
		return nil, errors.Wrap(err, "send command")
	}

	if !d.wait {
		return outMsg{"Processing by agents. Please check status later"}, nil
	}

	return waitForDelete(ctx, pbm, cid)
}

func parseOlderThan(s string) (primitive.Timestamp, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return primitive.Timestamp{}, errInvalidFormat
	}

	ts, err := parseTS(s)
	if !errors.Is(err, errInvalidFormat) {
		return ts, err
	}

	dur, err := parseDuration(s)
	if err != nil {
		if errors.Is(err, errInvalidDuration) {
			err = errInvalidFormat
		}
		return primitive.Timestamp{}, err
	}

	unix := time.Now().UTC().Add(-dur).Unix()
	return primitive.Timestamp{T: uint32(unix), I: 0}, nil
}

var errInvalidDuration = errors.New("invalid duration")

func parseDuration(s string) (time.Duration, error) {
	d, c := int64(0), ""
	_, err := fmt.Sscanf(s, "%d%s", &d, &c)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, errInvalidDuration
		}
		return 0, err
	}
	if c != "d" {
		return 0, errInvalidDuration
	}

	return time.Duration(d * 24 * int64(time.Hour)), nil
}

func printCleanupInfoTo(w io.Writer, backups []backup.BackupMeta, chunks []oplog.OplogChunk) {
	if len(backups) != 0 {
		fmt.Fprintln(w, "Snapshots:")
		for i := range backups {
			bcp := &backups[i]
			t := string(bcp.Type)
			if util.IsSelective(bcp.Namespaces) {
				t += ", selective"
			} else if bcp.Type == defs.IncrementalBackup && bcp.SrcBackup == "" {
				t += ", base"
			}
			fmt.Fprintf(w, " - %s <%s> [restore_time: %s]\n",
				bcp.Name, t, fmtTS(int64(bcp.LastWriteTS.T)))
		}
	}

	if len(chunks) == 0 {
		return
	}

	type oplogRange struct {
		Start, End primitive.Timestamp
	}

	oplogRanges := make(map[string][]oplogRange)
	for _, c := range chunks {
		rs := oplogRanges[c.RS]
		if rs == nil {
			oplogRanges[c.RS] = []oplogRange{{c.StartTS, c.EndTS}}
			continue
		}

		lastWrite := &rs[len(rs)-1].End
		if lastWrite.Compare(c.StartTS) == -1 {
			oplogRanges[c.RS] = append(rs, oplogRange{c.StartTS, c.EndTS})
			continue
		}
		if lastWrite.Compare(c.EndTS) == -1 {
			*lastWrite = c.EndTS
		}
	}

	fmt.Fprintln(w, "PITR chunks (by replset name):")
	for rs, ops := range oplogRanges {
		fmt.Fprintf(w, " %s:\n", rs)

		for _, r := range ops {
			fmt.Fprintf(w, " - %s - %s\n", fmtTS(int64(r.Start.T)), fmtTS(int64(r.End.T)))
		}
	}
}

func askCleanupConfirmation(info backup.CleanupInfo) error {
	printCleanupInfoTo(os.Stdout, info.Backups, info.Chunks)
	return askConfirmation("Are you sure you want to delete?")
}

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

func waitForDelete(ctx context.Context, pbm sdk.Client, cid sdk.CommandID) (fmt.Stringer, error) {
	progressCtx, stopProgress := context.WithCancel(ctx)
	defer stopProgress()

	go func() {
		fmt.Print("Waiting for delete to be done ")

		for tick := time.NewTicker(time.Second); ; {
			select {
			case <-tick.C:
				fmt.Print(".")
			case <-progressCtx.Done():
				return
			}
		}
	}()

	waitCtx, stopWaiting := context.WithTimeout(progressCtx, time.Minute)
	defer stopWaiting()

	err := sdk.WaitForBackupDelete(waitCtx, pbm)
	if err != nil {
		if !errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}

		waitCtx, stopWaiting := context.WithTimeout(progressCtx, time.Minute)
		defer stopWaiting()

		msg, err := sdk.WaitForErrorLog(waitCtx, pbm, cid)
		if err != nil {
			return nil, errors.Wrap(err, "read agents log")
		}
		if msg != "" {
			return nil, errors.New(msg)
		}

		return outMsg{"Operation is still in progress, please check status in a while"}, nil
	}

	stopProgress()
	fmt.Println("[done]")
	return runList(ctx, pbm, &listOpts{})
}
