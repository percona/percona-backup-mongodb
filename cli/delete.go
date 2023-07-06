package cli

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm"
)

type deleteBcpOpts struct {
	name      string
	olderThan string
	force     bool
}

func deleteBackup(pbmClient *pbm.PBM, d *deleteBcpOpts, outf outFormat) (fmt.Stringer, error) {
	if !d.force && isTTY() {
		fmt.Print("Are you sure you want delete backup(s)? [y/N] ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		switch strings.TrimSpace(scanner.Text()) {
		case "yes", "Yes", "YES", "Y", "y":
		default:
			return nil, nil
		}
	}

	cmd := pbm.Cmd{
		Cmd:    pbm.CmdDeleteBackup,
		Delete: &pbm.DeleteBackupCmd{},
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
	err := pbmClient.SendCmd(cmd)
	if err != nil {
		return nil, errors.Wrap(err, "schedule delete")
	}
	if outf != outText {
		return nil, nil
	}

	fmt.Print("Waiting for delete to be done ")
	err = waitOp(pbmClient,
		&pbm.LockHeader{
			Type: pbm.CmdDeleteBackup,
		},
		time.Second*60)
	if err != nil && err != errTout {
		return nil, err
	}

	errl, err := lastLogErr(pbmClient, pbm.CmdDeleteBackup, tsop)
	if err != nil {
		return nil, errors.Wrap(err, "read agents log")
	}

	if errl != "" {
		return nil, errors.New(errl)
	}

	if err == errTout {
		fmt.Println("\nOperation is still in progress, please check status in a while")
	} else {
		time.Sleep(time.Second)
		fmt.Print(".")
		time.Sleep(time.Second)
		fmt.Println("[done]")
	}

	return runList(pbmClient, &listOpts{})
}

type deletePitrOpts struct {
	olderThan string
	force     bool
	all       bool
}

func deletePITR(pbmClient *pbm.PBM, d *deletePitrOpts, outf outFormat) (fmt.Stringer, error) {
	if !d.all && len(d.olderThan) == 0 {
		return nil, errors.New("either --older-than or --all should be set")
	}

	if !d.force && isTTY() {
		all := ""
		if d.all {
			all = " ALL"
		}
		fmt.Printf("Are you sure you want delete%s chunks? [y/N] ", all)
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		switch strings.TrimSpace(scanner.Text()) {
		case "yes", "Yes", "YES", "Y", "y":
		default:
			return nil, nil
		}
	}

	cmd := pbm.Cmd{
		Cmd:        pbm.CmdDeletePITR,
		DeletePITR: &pbm.DeletePITRCmd{},
	}
	if !d.all && len(d.olderThan) > 0 {
		t, err := parseDateT(d.olderThan)
		if err != nil {
			return nil, errors.Wrap(err, "parse date")
		}
		cmd.DeletePITR.OlderThan = t.UTC().Unix()
	}
	tsop := time.Now().UTC().Unix()
	err := pbmClient.SendCmd(cmd)
	if err != nil {
		return nil, errors.Wrap(err, "schedule pitr delete")
	}
	if outf != outText {
		return nil, nil
	}

	fmt.Print("Waiting for delete to be done ")
	err = waitOp(pbmClient,
		&pbm.LockHeader{
			Type: pbm.CmdDeletePITR,
		},
		time.Second*60)
	if err != nil && err != errTout {
		return nil, err
	}

	errl, err := lastLogErr(pbmClient, pbm.CmdDeletePITR, tsop)
	if err != nil {
		return nil, errors.Wrap(err, "read agents log")
	}

	if errl != "" {
		return nil, errors.New(errl)
	}

	if err == errTout {
		fmt.Println("\nOperation is still in progress, please check status in a while")
	} else {
		time.Sleep(time.Second)
		fmt.Print(".")
		time.Sleep(time.Second)
		fmt.Println("[done]")
	}

	return runList(pbmClient, &listOpts{})
}

type cleanupOptions struct {
	olderThan string
	yes       bool
	wait      bool
	dryRun    bool
}

func retentionCleanup(pbmClient *pbm.PBM, d *cleanupOptions) (fmt.Stringer, error) {
	ts, err := parseOlderThan(d.olderThan)
	if err != nil {
		return nil, errors.Wrap(err, "parse --older-than")
	}
	info, err := pbm.MakeCleanupInfo(pbmClient.Context(), pbmClient.Conn, ts)
	if err != nil {
		return nil, errors.WithMessage(err, "make cleanup report")
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
		yes, err := askCleanupConfirmation(info)
		if err != nil {
			return nil, err
		}
		if !yes {
			return outMsg{"aborted"}, nil
		}
	}

	tsop := time.Now().Unix()
	err = pbmClient.SendCmd(pbm.Cmd{
		Cmd:     pbm.CmdCleanup,
		Cleanup: &pbm.CleanupCmd{OlderThan: ts},
	})
	if err != nil {
		return nil, errors.WithMessage(err, "send command")
	}
	if !d.wait {
		return outMsg{"Processing by agents. Please check status later"}, nil
	}

	fmt.Print("Waiting")
	err = waitOp(pbmClient, &pbm.LockHeader{Type: pbm.CmdCleanup}, 10*time.Minute)
	fmt.Println()
	if err != nil {
		if errors.Is(err, errTout) {
			return outMsg{"Operation is still in progress, please check status later"}, nil
		}
		return nil, err
	}

	errl, err := lastLogErr(pbmClient, pbm.CmdCleanup, tsop)
	if err != nil {
		return nil, errors.WithMessage(err, "read agents log")
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

func printCleanupInfoTo(w io.Writer, backups []pbm.BackupMeta, chunks []pbm.OplogChunk) {
	if len(backups) != 0 {
		fmt.Fprintln(w, "Snapshots:")
		for i := range backups {
			bcp := &backups[i]
			t := bcp.Type
			if len(bcp.Namespaces) != 0 {
				t += ", selective"
			} else if bcp.Type == pbm.IncrementalBackup && bcp.SrcBackup == "" {
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
		if primitive.CompareTimestamp(*lastWrite, c.StartTS) == -1 {
			oplogRanges[c.RS] = append(rs, oplogRange{c.StartTS, c.EndTS})
			continue
		}
		if primitive.CompareTimestamp(*lastWrite, c.EndTS) == -1 {
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

func askCleanupConfirmation(info pbm.CleanupInfo) (bool, error) {
	printCleanupInfoTo(os.Stdout, info.Backups, info.Chunks)

	if !isTTY() {
		return false, errors.New("no tty")
	}

	fmt.Print("Are you sure you want delete? [y/N] ")

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	switch strings.TrimSpace(scanner.Text()) {
	case "yes", "Yes", "YES", "Y", "y":
		return true, nil
	}

	return false, nil
}
