package main

import (
	"log"

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
			return errors.Errorf("another operation in progress, %s/%s", l.Type, l.BackupName)
		}
	}

	err = cn.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdRestore,
		Restore: pbm.RestoreCmd{
			BackupName: bcpName,
		},
	})
	if err != nil {
		return errors.Wrap(err, "send command")
	}

	return nil
}
