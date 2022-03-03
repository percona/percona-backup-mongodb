package cli

import (
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
)

func replayOplog(cn *pbm.PBM, o replayOptions, outf outFormat) (fmt.Stringer, error) {
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
		Replay: pbm.ReplayCmd{
			Name:  name,
			Start: startTS,
			End:   endTS,
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
