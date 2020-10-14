package main

import (
	"strings"

	"github.com/percona/percona-backup-mongodb/pbm"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
)

func logs(cn *pbm.PBM) {
	r := plog.LogRequest{}

	if *logsNodeF != "" {
		n := strings.Split(*logsNodeF, "/")
		r.RS = n[0]
		if len(n) > 1 {
			r.Node = n[1]
		}
	}
}
