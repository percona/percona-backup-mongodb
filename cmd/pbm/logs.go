package main

import (
	"log"
	"os"
	"strings"

	"github.com/percona/percona-backup-mongodb/pbm"
	plog "github.com/percona/percona-backup-mongodb/pbm/log"
)

func logs(cn *pbm.PBM) {
	r := &plog.LogRequest{}

	if *logsNodeF != "" {
		n := strings.Split(*logsNodeF, "/")
		r.RS = n[0]
		if len(n) > 1 {
			r.Node = n[1]
		}
	}

	if *logsEventF != "" {
		e := strings.Split(*logsEventF, "/")
		r.Event = e[0]
		if len(e) > 1 {
			r.ObjName = e[1]
		}
	}

	if *logsOPIDF != "" {
		r.OPID = *logsOPIDF
	}

	switch *logsTypeF {
	case "F":
		r.Severity = plog.Fatal
	case "E":
		r.Severity = plog.Error
	case "W":
		r.Severity = plog.Warning
	case "I":
		r.Severity = plog.Info
	case "D":
		r.Severity = plog.Debug
	default:
		r.Severity = plog.Info
	}

	f := plog.FormatText
	if *logsOutF == "json" {
		f = plog.FormatJSON
	}

	err := cn.Logger().PrintLogs(os.Stdout, f, r, *logsTailF, r.Node == "")

	if err != nil {
		log.Fatalf("Error: get logs: %v", err)
	}
}
