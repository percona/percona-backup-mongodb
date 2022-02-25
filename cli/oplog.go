package cli

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func replayOplog(cn *pbm.PBM, o replayOptions, outf outFormat) (fmt.Stringer, error) {
	if err := checkConcurrentOp(cn); err != nil {
		return nil, err
	}

	startTS, err := parseDateT(o.start)
	if err != nil {
		return nil, errors.Wrap(err, "parse start time")
	}
	endTS, err := parseDateT(o.end)
	if err != nil {
		return nil, errors.Wrap(err, "parse end time")
	}

	name := time.Now().UTC().Format(time.RFC3339Nano)
	cmd := pbm.Cmd{
		Cmd: pbm.CmdReplay,
		Replay: pbm.ReplayCmd{
			Name:  name,
			Start: startTS.Unix(),
			End:   endTS.Unix(),
		},
	}
	if err := cn.SendCmd(cmd); err != nil {
		return nil, errors.Wrap(err, "send command")
	}

	if outf != outText {
		return nil, nil
	}

	// todo(wait for status)

	return oplogReplayResult{Name: name}, nil
}
