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

	"github.com/percona/percona-backup-mongodb/internal"
	"github.com/percona/percona-backup-mongodb/internal/backup"
	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/ctrl"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/lock"
	"github.com/percona/percona-backup-mongodb/internal/oplog"
	"github.com/percona/percona-backup-mongodb/internal/util"
)

type deleteBcpOpts struct {
	name      string
	olderThan string
	force     bool
}

func deleteBackup(ctx context.Context, conn connect.Client, d *deleteBcpOpts, outf outFormat) (fmt.Stringer, error) {
	if !d.force {
		if err := askConfirmation("Are you sure you want to delete backup(s)?"); err != nil {
			if errors.Is(err, errUserCanceled) {
				return outMsg{err.Error()}, nil
			}
			return nil, err
		}
	}

	cmd := ctrl.Cmd{
		Cmd:    ctrl.CmdDeleteBackup,
		Delete: &ctrl.DeleteBackupCmd{},
	}
	if len(d.olderThan) > 0 {
		t, err := parseDateT(d.olderThan)
		if err != nil {
			return nil, errors.Wrap(err, "parse date")
		}
		cmd.Delete.OlderThan = t.UTC().Unix()
	} else {
		if len(d.name) == 0 {
			return nil, errors.New("backup name should be specified")
		}
		cmd.Delete.Backup = d.name
	}
	tsop := time.Now().UTC().Unix()
	err := sendCmd(ctx, conn, cmd)
	if err != nil {
		return nil, errors.Wrap(err, "schedule delete")
	}
	if outf != outText {
		return nil, nil
	}

	fmt.Print("Waiting for delete to be done ")
	err = waitOp(ctx,
		conn,
		&lock.LockHeader{Type: ctrl.CmdDeleteBackup},
		time.Second*60)
	if err != nil && !errors.Is(err, errTout) {
		return nil, err
	}

	errl, err := lastLogErr(ctx, conn, ctrl.CmdDeleteBackup, tsop)
	if err != nil {
		return nil, errors.Wrap(err, "read agents log")
	}

	if errl != "" {
		return nil, errors.New(errl)
	}

	if errors.Is(err, errTout) {
		fmt.Println("\nOperation is still in progress, please check status in a while")
	} else {
		time.Sleep(time.Second)
		fmt.Print(".")
		time.Sleep(time.Second)
		fmt.Println("[done]")
	}

	return runList(ctx, conn, &listOpts{})
}

type deletePitrOpts struct {
	olderThan string
	force     bool
	all       bool
}

func deletePITR(ctx context.Context, conn connect.Client, d *deletePitrOpts, outf outFormat) (fmt.Stringer, error) {
	if !d.all && len(d.olderThan) == 0 {
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

	cmd := ctrl.Cmd{
		Cmd:        ctrl.CmdDeletePITR,
		DeletePITR: &ctrl.DeletePITRCmd{},
	}
	if !d.all && len(d.olderThan) > 0 {
		t, err := parseDateT(d.olderThan)
		if err != nil {
			return nil, errors.Wrap(err, "parse date")
		}
		cmd.DeletePITR.OlderThan = t.UTC().Unix()
	}
	tsop := time.Now().UTC().Unix()
	err := sendCmd(ctx, conn, cmd)
	if err != nil {
		return nil, errors.Wrap(err, "schedule pitr delete")
	}
	if outf != outText {
		return nil, nil
	}

	fmt.Print("Waiting for delete to be done ")
	err = waitOp(ctx,
		conn,
		&lock.LockHeader{Type: ctrl.CmdDeletePITR},
		time.Second*60)
	if err != nil && !errors.Is(err, errTout) {
		return nil, err
	}

	errl, err := lastLogErr(ctx, conn, ctrl.CmdDeletePITR, tsop)
	if err != nil {
		return nil, errors.Wrap(err, "read agents log")
	}

	if errl != "" {
		return nil, errors.New(errl)
	}

	if errors.Is(err, errTout) {
		fmt.Println("\nOperation is still in progress, please check status in a while")
	} else {
		time.Sleep(time.Second)
		fmt.Print(".")
		time.Sleep(time.Second)
		fmt.Println("[done]")
	}

	return runList(ctx, conn, &listOpts{})
}

type cleanupOptions struct {
	olderThan string
	yes       bool
	wait      bool
	dryRun    bool
}

func retentionCleanup(ctx context.Context, conn connect.Client, d *cleanupOptions) (fmt.Stringer, error) {
	ts, err := parseOlderThan(d.olderThan)
	if err != nil {
		return nil, errors.Wrap(err, "parse --older-than")
	}
	info, err := internal.MakeCleanupInfo(ctx, conn, ts)
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

	tsop := time.Now().Unix()
	err = sendCmd(ctx, conn, ctrl.Cmd{
		Cmd:     ctrl.CmdCleanup,
		Cleanup: &ctrl.CleanupCmd{OlderThan: ts},
	})
	if err != nil {
		return nil, errors.Wrap(err, "send command")
	}
	if !d.wait {
		return outMsg{"Processing by agents. Please check status later"}, nil
	}

	fmt.Print("Waiting")
	err = waitOp(ctx,
		conn,
		&lock.LockHeader{Type: ctrl.CmdCleanup},
		10*time.Minute)
	fmt.Println()
	if err != nil {
		if errors.Is(err, errTout) {
			return outMsg{"Operation is still in progress, please check status later"}, nil
		}
		return nil, err
	}

	errl, err := lastLogErr(ctx, conn, ctrl.CmdCleanup, tsop)
	if err != nil {
		return nil, errors.Wrap(err, "read agents log")
	}
	if errl != "" {
		return nil, errors.New(errl)
	}

	return outMsg{"Done"}, nil
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

func askCleanupConfirmation(info internal.CleanupInfo) error {
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
