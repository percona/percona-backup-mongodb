package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
)

type replayOptions struct {
	start string
	end   string
	wait  bool
	rsMap string
}

type oplogReplayResult struct {
	Name string `json:"name"`
	done bool
	err  string
}

func (r oplogReplayResult) HasError() bool {
	return r.err != ""
}

func (r oplogReplayResult) String() string {
	if r.done {
		return "\nOplog replay successfully finished!\n"
	}
	if r.err != "" {
		return "\n Error: " + r.err
	}
	return fmt.Sprintf("Oplog replay %q has started", r.Name)
}

func replayOplog(cn *pbm.PBM, o replayOptions, outf outFormat) (fmt.Stringer, error) {
	rsMap, err := parseRSNamesMapping(o.rsMap)
	if err != nil {
		return nil, errors.WithMessage(err, "cannot parse replset mapping")
	}

	startTS, err := parseTS(o.start)
	if err != nil {
		return nil, errors.Wrap(err, "parse start time")
	}
	endTS, err := parseTS(o.end)
	if err != nil {
		return nil, errors.Wrap(err, "parse end time")
	}

	err = checkConcurrentOp(cn)
	if err != nil {
		return nil, err
	}

	name := time.Now().UTC().Format(time.RFC3339Nano)
	cmd := pbm.Cmd{
		Cmd: pbm.CmdReplay,
		Replay: &pbm.ReplayCmd{
			Name:  name,
			Start: startTS,
			End:   endTS,
			RSMap: rsMap,
		},
	}
	if err := cn.SendCmd(cmd); err != nil {
		return nil, errors.Wrap(err, "send command")
	}

	if outf != outText {
		return oplogReplayResult{Name: name}, nil
	}

	fmt.Printf("Starting oplog replay '%s - %s'", o.start, o.end)

	ctx, cancel := context.WithTimeout(context.Background(), pbm.WaitActionStart)
	defer cancel()

	m, err := waitForRestoreStatus(ctx, cn, name, cn.GetRestoreMeta)
	if err != nil {
		return nil, err
	}

	if !o.wait || m == nil {
		return oplogReplayResult{Name: name}, nil
	}

	fmt.Print("Started.\nWaiting to finish")
	err = waitRestore(cn, m, pbm.StatusDone, 0)
	if err != nil {
		return oplogReplayResult{err: err.Error()}, nil
	}

	return oplogReplayResult{Name: name, done: true}, nil
}
