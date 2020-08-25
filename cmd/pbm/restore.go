package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func restore(cn *pbm.PBM, bcpName string) error {
	bcp, err := cn.GetBackupMeta(bcpName)
	if err != nil {
		return errors.Wrap(err, "get backup data")
	}
	if bcp.Name != bcpName {
		return errors.Errorf("backup '%s' not found", bcpName)
	}
	if bcp.Status != pbm.StatusDone {
		return errors.Errorf("backup '%s' isn't finished successfully", bcpName)
	}

	locks, err := cn.GetLocks(&pbm.LockHeader{})
	if err != nil {
		log.Println("get locks", err)
	}

	ts, err := cn.ClusterTime()
	if err != nil {
		return errors.Wrap(err, "read cluster time")
	}

	// Stop if there is some live operation.
	// But in case of stale lock just move on
	// and leave it for agents to deal with.
	for _, l := range locks {
		if l.Heartbeat.T+pbm.StaleFrameSec >= ts.T {
			return errors.Errorf("another operation in progress, %s/%s [%s/%s]", l.Type, l.BackupName, l.Replset, l.Node)
		}
	}

	name := time.Now().UTC().Format(time.RFC3339Nano)
	err = cn.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdRestore,
		Restore: pbm.RestoreCmd{
			Name:       name,
			BackupName: bcpName,
		},
	})
	if err != nil {
		return errors.Wrap(err, "send command")
	}

	ctx, cancel := context.WithTimeout(context.Background(), pbm.WaitActionStart)
	defer cancel()

	return waitForRestoreStatus(ctx, cn, name)
}

func pitrestore(cn *pbm.PBM, t string) error {
	tsto, err := parseDateT(t)
	if err != nil {
		return errors.Wrap(err, "parse date")
	}

	locks, err := cn.GetLocks(&pbm.LockHeader{})
	if err != nil {
		log.Println("get locks", err)
	}

	ts, err := cn.ClusterTime()
	if err != nil {
		return errors.Wrap(err, "read cluster time")
	}

	// Stop if there is some live operation.
	// But in case of stale lock just move on
	// and leave it for agents to deal with.
	for _, l := range locks {
		if l.Heartbeat.T+pbm.StaleFrameSec >= ts.T {
			return errors.Errorf("another operation in progress, %s/%s [%s/%s]", l.Type, l.BackupName, l.Replset, l.Node)
		}
	}

	name := time.Now().UTC().Format(time.RFC3339Nano)
	err = cn.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdPITRestore,
		PITRestore: pbm.PITRestoreCmd{
			Name: name,
			TS:   tsto.Unix(),
		},
	})
	if err != nil {
		return errors.Wrap(err, "send command")
	}

	ctx, cancel := context.WithTimeout(context.Background(), pbm.WaitActionStart)
	defer cancel()

	return waitForRestoreStatus(ctx, cn, name)
}

func waitForRestoreStatus(ctx context.Context, cn *pbm.PBM, name string) error {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	var err error
	meta := new(pbm.RestoreMeta)
	for {
		select {
		case <-tk.C:
			fmt.Print(".")
			meta, err = cn.GetRestoreMeta(name)
			if err != nil {
				return errors.Wrap(err, "get metadata")
			}
			switch meta.Status {
			case pbm.StatusRunning, pbm.StatusDumpDone, pbm.StatusDone:
				return nil
			case pbm.StatusError:
				rs := ""
				for _, s := range meta.Replsets {
					rs += fmt.Sprintf("\n- Restore on replicaset \"%s\" in state: %v", s.Name, s.Status)
					if s.Error != "" {
						rs += ": " + s.Error
					}
				}
				return errors.New(meta.Error + rs)
			}
		case <-ctx.Done():
			rs := ""
			for _, s := range meta.Replsets {
				rs += fmt.Sprintf("- Restore on replicaset \"%s\" in state: %v\n", s.Name, s.Status)
				if s.Error != "" {
					rs += ": " + s.Error
				}
			}
			if rs == "" {
				rs = "<no replset has started restore>\n"
			}

			return errors.New("no confirmation that restore has successfully started. Replsets status:\n" + rs)
		}
	}
}

func printRestoreList(cn *pbm.PBM, size int64, full bool) {
	rs, err := cn.RestoresList(size)
	if err != nil {
		log.Fatalln("Error: unable to get restore list:", err)
	}
	fmt.Println("Restores history:")
	for i := len(rs) - 1; i >= 0; i-- {
		r := rs[i]

		var rprint, name string

		if r.PITR == 0 {
			name = r.Backup
		} else {
			name = "PITR: " + time.Unix(r.PITR, 0).UTC().Format(time.RFC3339)
		}
		if full {
			name += fmt.Sprintf(" [%s]", r.Name)
		}
		switch r.Status {
		case pbm.StatusDone:
			rprint = name
		case pbm.StatusError:
			rprint = fmt.Sprintf("%s\tFailed with \"%s\"", name, r.Error)
		default:
			rprint, err = printRestoreProgress(r, cn, full)
			if err != nil {
				log.Fatalf("Error: list restores %s: %v\n", name, err)
			}
		}

		fmt.Println(" ", rprint)
	}
}

func printRestoreProgress(r pbm.RestoreMeta, pbmClient *pbm.PBM, full bool) (string, error) {
	locks, err := pbmClient.GetLocks(&pbm.LockHeader{
		Type:       pbm.CmdRestore,
		BackupName: r.Name,
	})

	if err != nil {
		return "", errors.Wrap(err, "get locks")
	}

	ts, err := pbmClient.ClusterTime()
	if err != nil {
		return "", errors.Wrap(err, "read cluster time")
	}

	name := r.Backup
	if full {
		name += fmt.Sprintf(" [%s]", r.Name)
	}
	stale := false
	staleMsg := "Stale: pbm-agents make no progress:"
	for _, l := range locks {
		if l.Heartbeat.T+pbm.StaleFrameSec < ts.T {
			stale = true
			staleMsg += fmt.Sprintf(" %s/%s [%s],", l.Replset, l.Node, time.Unix(int64(l.Heartbeat.T), 0).Format(time.RFC3339))
		}
	}

	if stale {
		return fmt.Sprintf("%s\t%s", name, staleMsg[:len(staleMsg)-1]), nil
	}

	return fmt.Sprintf("%s\tIn progress [%s] (Launched at %s)", name, r.Status, time.Unix(r.StartTS, 0).Format(time.RFC3339)), nil
}
