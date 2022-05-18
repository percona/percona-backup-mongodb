package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/client"
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
		Cmd: pbm.CmdDeleteBackup,
	}
	cmd.Delete = &pbm.DeleteBackupCmd{}
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

func doDeleteBackup(ctx context.Context, pbmClient *pbm.PBM, c *client.Client, d *deleteBcpOpts, outf outFormat) (fmt.Stringer, error) {
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

	var res client.Result

	if len(d.olderThan) > 0 {
		t, err := parseDateT(d.olderThan)
		if err != nil {
			return nil, errors.Wrap(err, "parse date")
		}
		res = c.DeleteManyBackups(ctx, &client.DeleteManyBackupsOptions{OlderThan: t})
	} else {
		if d.name == "" {
			return nil, errors.New("backup name should be specified")
		}
		res = c.DeleteBackup(ctx, d.name)
	}

	if err := res.Err(); err != nil {
		return nil, errors.WithMessage(err, "schedule delete")
	}

	if outf != outText {
		return nil, nil
	}

	fmt.Print("Waiting for delete to be done ")

	wait60s, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	if err := client.WaitForDeleteBackupFinish(wait60s, c, res.ID()); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			fmt.Println("\nOperation is still in progress, please check status in a while")
		}

		return nil, err
	}

	time.Sleep(time.Second)
	fmt.Print(".")
	time.Sleep(time.Second)
	fmt.Println("[done]")

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
		Cmd: pbm.CmdDeletePITR,
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
