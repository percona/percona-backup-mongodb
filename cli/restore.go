package cli

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm"
)

type restoreOpts struct {
	bcp      string
	pitr     string
	pitrBase string
}

type restoreRet struct {
	Snapshot string `json:"snapshot,omitempty"`
	PITR     string `json:"point-in-time,omitempty"`
}

func (r restoreRet) String() string {
	if r.Snapshot != "" {
		return fmt.Sprintf("Restore of the snapshot from '%s' has started", r.Snapshot)
	}
	if r.PITR != "" {
		return fmt.Sprintf("Restore to the point in time '%s' has started", r.PITR)
	}

	return ""
}

type replayOptions struct {
	start string
	end   string
}

type oplogReplayResult struct {
	Name string `json:"name"`
}

func (r oplogReplayResult) String() string {
	return fmt.Sprintf("Oplog replay %q has started", r.Name)
}

func runRestore(cn *pbm.PBM, o *restoreOpts, outf outFormat) (fmt.Stringer, error) {
	if o.pitr != "" && o.bcp != "" {
		return nil, errors.New("either a backup name or point in time should be set, non both together!")
	}

	switch {
	case o.bcp != "":
		err := restore(cn, o.bcp, outf)
		if err != nil {
			return nil, err
		}
		return restoreRet{Snapshot: o.bcp}, nil
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

func restore(cn *pbm.PBM, bcpName string, outf outFormat) error {
	bcp, err := cn.GetBackupMeta(bcpName)
	if errors.Is(err, pbm.ErrNotFound) {
		return errors.Errorf("backup '%s' not found", bcpName)
	}
	if err != nil {
		return errors.Wrap(err, "get backup data")
	}
	if bcp.Status != pbm.StatusDone {
		return errors.Errorf("backup '%s' didn't finish successfully", bcpName)
	}

	err = checkConcurrentOp(cn)
	if err != nil {
		return err
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

	if outf != outText {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), pbm.WaitActionStart)
	defer cancel()

	return waitForRestoreStatus(ctx, cn, name)
}

func parseTS(t string) (ts primitive.Timestamp, err error) {
	if si := strings.SplitN(t, ",", 2); len(si) == 2 {
		tt, err := strconv.ParseInt(si[0], 10, 64)
		if err != nil {
			return ts, errors.Wrap(err, "parse clusterTime T")
		}
		ti, err := strconv.ParseInt(si[1], 10, 64)
		if err != nil {
			return ts, errors.Wrap(err, "parse clusterTime I")
		}

		return primitive.Timestamp{T: uint32(tt), I: uint32(ti)}, nil
	}

	tsto, err := parseDateT(t)
	if err != nil {
		return ts, errors.Wrap(err, "parse date")
	}

	return primitive.Timestamp{T: uint32(tsto.Unix()), I: 0}, nil
}

func pitrestore(cn *pbm.PBM, t, base string, outf outFormat) (err error) {
	ts, err := parseTS(t)
	if err != nil {
		return err
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
			TS:   int64(ts.T),
			I:    int64(ts.I),
			Bcp:  base,
		},
	})
	if err != nil {
		return errors.Wrap(err, "send command")
	}

	if outf != outText {
		return nil
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
			if errors.Is(err, pbm.ErrNotFound) {
				continue
			}
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
