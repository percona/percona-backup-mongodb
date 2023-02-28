package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
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

type deleteAllOptions struct {
	olderThan string
	yes       bool
	wait      bool
	wtimeout  uint32
}

func deleteAll(pbmClient *pbm.PBM, d *deleteAllOptions, _ outFormat) (fmt.Stringer, error) {
	ctx, m := pbmClient.Context(), pbmClient.Conn

	ts, err := parseOlderThan(d.olderThan)
	if err != nil {
		return nil, errors.Wrap(err, "parse `older than` value")
	}
	cfg, err := pbmClient.GetConfig()
	if err != nil {
		return nil, errors.WithMessage(err, "get config")
	}
	ts, err = findAdjustedTS(ctx, m, ts, cfg.PITR.Enabled && !cfg.PITR.OplogOnly)
	if err != nil {
		return nil, errors.WithMessage(err, "find proper timestamp")
	}
	if ts.IsZero() {
		return nil, errors.New("deletion not allowed")
	}

	if !d.yes {
		err := askDeleteAllConfirmation(ctx, m, ts)
		if err != nil {
			if errors.Is(err, ErrAborted) {
				return outMsg{"aborted"}, nil
			}
			return nil, err
		}
	}

	tsop := time.Now().Unix()
	err = pbmClient.SendCmd(pbm.Cmd{
		Cmd:       pbm.CmdDeleteAll,
		DeleteAll: &pbm.DeleteAllCmd{OlderThan: ts},
	})
	if err != nil {
		return nil, errors.WithMessage(err, "send command")
	}
	if !d.wait {
		return outMsg{"Processing by agents. Please check status later"}, nil
	}

	fmt.Print("Waiting")
	wtimeout := time.Duration(d.wtimeout)
	if wtimeout == 0 {
		wtimeout = 60
	}
	err = waitOp(pbmClient, &pbm.LockHeader{Type: pbm.CmdDeleteAll}, wtimeout*time.Second)
	fmt.Println()
	if err != nil {
		if errors.Is(err, errTout) {
			return outMsg{"Operation is still in progress, please check status later"}, nil
		}
		return nil, err
	}

	errl, err := lastLogErr(pbmClient, pbm.CmdDeleteAll, tsop)
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
		return primitive.Timestamp{}, errInvalidDateTimeFormat
	}

	ts, err := parseTS(s)
	if !errors.Is(err, errInvalidDateTimeFormat) {
		return ts, err
	}

	dur, err := parseDuration(s)
	if err != nil {
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
		return 0, err
	}
	if c != "d" {
		return 0, errInvalidDuration
	}

	return time.Duration(d * 24 * int64(time.Hour)), nil
}

func printDeleteAllInfo(backups []pbm.BackupMeta, chunks []pbm.OplogChunk) {
	fmt.Println("Snapshots:")
	if len(backups) == 0 {
		fmt.Println("nothing to delete")
	} else {
		for i := range backups {
			bcp := &backups[i]
			t := bcp.Type
			if len(bcp.Namespaces) != 0 {
				t += ", selective"
			}

			fmt.Printf(" - %s <%s> [restore_time: %s]\n",
				bcp.Name, t, fmtTS(int64(bcp.LastWriteTS.T)))
		}
	}

	fmt.Println("PITR chunks (by replset name):")
	if len(chunks) == 0 {
		fmt.Println("nothing to delete")
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

	for rs, ops := range oplogRanges {
		fmt.Printf(" %s:\n", rs)

		for _, r := range ops {
			fmt.Printf(" - %d,%d - %d,%d\n",
				r.Start.T, r.Start.I, r.End.T, r.End.I)
		}
	}
}

var ErrAborted = errors.New("aborted")

func askDeleteAllConfirmation(ctx context.Context, m *mongo.Client, ts primitive.Timestamp) error {
	if !isTTY() {
		return errors.New("no tty")
	}

	backups, err := backup.ListBackupsBefore(ctx, m, ts)
	if err != nil {
		return errors.WithMessage(err, "list backups")
	}
	chunks, err := oplog.ListChunksBefore(ctx, m, ts)
	if err != nil {
		return errors.WithMessage(err, "list oplog chunks")
	}

	printDeleteAllInfo(backups, chunks)

	fmt.Print("Are you sure you want delete? [y/N] ")

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	switch strings.TrimSpace(scanner.Text()) {
	case "yes", "Yes", "YES", "Y", "y":
		return nil
	}

	return ErrAborted
}

// findAdjustedTS returns a timestamp of the restore time of any following backup.
func findAdjustedTS(ctx context.Context, m *mongo.Client, ts primitive.Timestamp, strict bool) (primitive.Timestamp, error) {
	backup, err := findBackupSince(ctx, m, ts, false)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return primitive.Timestamp{}, err
		}

		if strict {
			ts = primitive.Timestamp{}
		}
		return ts, nil
	}

	if !strict || len(backup.Namespaces) == 0 {
		return backup.LastWriteTS, nil
	}

	// ensure there is a base snapshot for full PITR
	_, err = findBackupSince(ctx, m, backup.LastWriteTS, true)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return primitive.Timestamp{}, nil
	}

	return backup.LastWriteTS, nil
}

// findBackupSince returns a backup with restore time <= ts.
// If fullOnly is true, only full backup (non-selective) will be returned.
// Returned backup can be physical, logical.
func findBackupSince(ctx context.Context, m *mongo.Client, ts primitive.Timestamp, fullOnly bool) (*pbm.BackupMeta, error) {
	filter := bson.D{{"last_write_ts", bson.M{"$gte": ts}}}
	if fullOnly {
		filter = append(filter, bson.E{"nss", nil})
	}
	opts := options.FindOne() //.SetProjection(bson.D{{"last_write_ts", 1}})
	cur := m.Database(pbm.DB).Collection(pbm.BcpCollection).FindOne(ctx, filter, opts)
	if err := cur.Err(); err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	rv := &pbm.BackupMeta{}
	err := cur.Decode(&rv)
	return rv, errors.WithMessage(err, "decode")
}
