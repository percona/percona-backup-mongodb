package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

type restoreOpts struct {
	bcp      string
	pitr     string
	pitrBase string
	wait     bool
}

type restoreRet struct {
	Snapshot string `json:"snapshot,omitempty"`
	PITR     string `json:"point-in-time,omitempty"`
	Leader   string `json:"leader,omitempty"`
	done     bool
	physical bool
	err      string
}

func (r restoreRet) String() string {
	switch {
	case r.done:
		m := "\nRestore successfully finished!\n"
		if r.physical {
			m += "Restart the cluster and pbm-agents, and run `pbm config --force-resync`"
		}
		return m
	case r.err != "":
		return "\n Error: " + r.err
	case r.Snapshot != "":
		var l string
		if r.Leader != "" {
			l = ". Leader: " + r.Leader + "\n"
		}
		return fmt.Sprintf("Restore of the snapshot from '%s' has started%s", r.Snapshot, l)
	case r.PITR != "":
		return fmt.Sprintf("Restore to the point in time '%s' has started", r.PITR)

	default:
		return ""
	}
}

func runRestore(cn *pbm.PBM, o *restoreOpts, outf outFormat) (fmt.Stringer, error) {
	if o.pitr != "" && o.bcp != "" {
		return nil, errors.New("either a backup name or point in time should be set, non both together!")
	}

	switch {
	case o.bcp != "":
		m, err := restore(cn, o.bcp, outf)
		if err != nil {
			return nil, err
		}
		if !o.wait {
			return restoreRet{Snapshot: o.bcp, Leader: m.Leader}, nil
		}

		typ := " logical restore.\nWaiting to finish"
		if m.Type == pbm.PhysicalBackup {
			typ = fmt.Sprintf(" physical restore. Leader: %s\nWaiting to finish", m.Leader)
		}
		fmt.Printf("Started%s", typ)
		err = waitRestore(cn, m)
		if err == nil {
			return restoreRet{
				done:     true,
				physical: m.Type == pbm.PhysicalBackup,
			}, nil
		}

		if serr, ok := err.(errRestoreFailed); ok {
			return restoreRet{err: serr.Error()}, nil
		}
		return restoreRet{err: fmt.Sprintf("%s.\n Try to check logs on node %s", err.Error(), m.Leader)}, nil
	case o.pitr != "":
		err := pitrestore(cn, o.pitr, o.pitrBase, outf)
		if err != nil {
			return nil, err
		}
		return restoreRet{PITR: o.pitr}, nil
	default:
		return nil, errors.New("undefined restore state")
	}
}

func waitRestore(cn *pbm.PBM, m *pbm.RestoreMeta) error {
	ep, _ := cn.GetEpoch()
	stg, err := cn.GetStorage(cn.Logger().NewEvent(string(pbm.CmdRestore), m.Backup, m.OPID, ep.TS()))
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()

	fname := m.Name
	var rmeta *pbm.RestoreMeta

	getMeta := cn.GetRestoreMeta
	if m.Type == pbm.PhysicalBackup {
		fname = fmt.Sprintf("%s/%s.json", pbm.PhysRestoresDir, m.Name)
		getMeta = func(name string) (*pbm.RestoreMeta, error) {
			return getRestoreMetaStg(cn, name, stg)
		}
	}

	for range tk.C {
		fmt.Print(".")
		rmeta, err = getMeta(fname)
		if errors.Is(err, pbm.ErrNotFound) {
			continue
		}
		if err != nil {
			return errors.Wrap(err, "get restore metadata")
		}

		switch rmeta.Status {
		case pbm.StatusDone:
			return nil
		case pbm.StatusError:
			return errRestoreFailed{fmt.Sprintf("restore failed with: %s", rmeta.Error)}
		}
	}

	return nil
}

func getRestoreMetaStg(cn *pbm.PBM, name string, stg storage.Storage) (*pbm.RestoreMeta, error) {
	_, err := stg.FileStat(name)
	if err == storage.ErrNotExist {
		return nil, pbm.ErrNotFound
	}
	if err != nil {
		return nil, errors.Wrap(err, "get stat")
	}

	src, err := stg.SourceReader(name)
	if err != nil {
		return nil, errors.Wrapf(err, "get file %s", name)
	}

	rmeta := new(pbm.RestoreMeta)
	err = json.NewDecoder(src).Decode(rmeta)
	if err != nil {
		return nil, errors.Wrapf(err, "decode meta %s", name)
	}

	return rmeta, nil
}

type errRestoreFailed struct {
	string
}

func (e errRestoreFailed) Error() string {
	return e.string
}

func restore(cn *pbm.PBM, bcpName string, outf outFormat) (*pbm.RestoreMeta, error) {
	bcp, err := cn.GetBackupMeta(bcpName)
	if errors.Is(err, pbm.ErrNotFound) {
		return nil, errors.Errorf("backup '%s' not found", bcpName)
	}
	if err != nil {
		return nil, errors.Wrap(err, "get backup data")
	}
	if bcp.Status != pbm.StatusDone {
		return nil, errors.Errorf("backup '%s' didn't finish successfully", bcpName)
	}

	err = checkConcurrentOp(cn)
	if err != nil {
		return nil, err
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
		return nil, errors.Wrap(err, "send command")
	}

	if outf != outText {
		return nil, nil
	}

	fmt.Printf("Starting restore from '%s'", bcpName)

	ctx, cancel := context.WithTimeout(context.Background(), pbm.WaitActionStart)
	defer cancel()

	return waitForRestoreStatus(ctx, cn, name)
}

func pitrestore(cn *pbm.PBM, t, base string, outf outFormat) error {
	tsto, err := parseDateT(t)
	if err != nil {
		return errors.Wrap(err, "parse date")
	}

	err = checkConcurrentOp(cn)
	if err != nil {
		return err
	}

	name := time.Now().UTC().Format(time.RFC3339Nano)
	err = cn.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdPITRestore,
		PITRestore: pbm.PITRestoreCmd{
			Name: name,
			TS:   tsto.Unix(),
			Bcp:  base,
		},
	})
	if err != nil {
		return errors.Wrap(err, "send command")
	}

	if outf != outText {
		return nil
	}

	fmt.Printf("Starting restore to the point in time '%s'", t)

	ctx, cancel := context.WithTimeout(context.Background(), pbm.WaitActionStart)
	defer cancel()

	_, err = waitForRestoreStatus(ctx, cn, name)
	return err
}

func waitForRestoreStatus(ctx context.Context, cn *pbm.PBM, name string) (*pbm.RestoreMeta, error) {
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	var err error
	meta := new(pbm.RestoreMeta)
	for {
		select {
		case <-tk.C:
			fmt.Print(".")
			meta, err = cn.GetRestoreMeta(name)
			if errors.Is(err, pbm.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, errors.Wrap(err, "get metadata")
			}
			if meta == nil {
				continue
			}
			switch meta.Status {
			case pbm.StatusRunning, pbm.StatusDumpDone, pbm.StatusDone:
				return meta, nil
			case pbm.StatusError:
				rs := ""
				for _, s := range meta.Replsets {
					rs += fmt.Sprintf("\n- Restore on replicaset \"%s\" in state: %v", s.Name, s.Status)
					if s.Error != "" {
						rs += ": " + s.Error
					}
				}
				return nil, errors.New(meta.Error + rs)
			}
		case <-ctx.Done():
			rs := ""
			if meta != nil {
				for _, s := range meta.Replsets {
					rs += fmt.Sprintf("- Restore on replicaset \"%s\" in state: %v\n", s.Name, s.Status)
					if s.Error != "" {
						rs += ": " + s.Error
					}
				}
			}
			if rs == "" {
				rs = "<no replset has started restore>\n"
			}

			return nil, errors.New("no confirmation that restore has successfully started. Replsets status:\n" + rs)
		}
	}
}
