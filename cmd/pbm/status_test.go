package main

import (
	"strings"
	"testing"

	"github.com/percona/percona-backup-mongodb/pbm/defs"
)

func TestStorageStatSnapshotDuration(t *testing.T) {
	// 1755000150: 2025-08-12T12:02:30Z
	const restoreTS = int64(1755000150)

	s := storageStat{
		Type: "filesystem",
		Path: "/data/pbm",
		Snapshot: []snapshotStat{
			{
				Name:      "2025-08-12T12:00:00Z",
				Status:    defs.StatusDone,
				Type:      defs.LogicalBackup,
				RestoreTS: restoreTS,
				Duration:  "2m30s",
			},
			{
				Name:      "2025-08-12T12:10:00Z",
				Status:    defs.StatusRunning,
				Type:      defs.LogicalBackup,
				RestoreTS: restoreTS,
			},
		},
	}

	lines := strings.Split(s.String(), "\n")

	var hdr, done, running string
	for _, l := range lines {
		switch {
		case strings.Contains(l, "RESTORE TIME"):
			hdr = l
		case strings.Contains(l, "2025-08-12T12:00:00Z"):
			done = l
		case strings.Contains(l, "2025-08-12T12:10:00Z"):
			running = l
		}
	}

	if !strings.Contains(hdr, "DURATION") {
		t.Errorf("header is missing the DURATION column: %q", hdr)
	}
	if !strings.Contains(done, "2m30s") {
		t.Errorf("done backup line is missing the duration: %q", done)
	}
	if !strings.Contains(running, " - ") {
		t.Errorf("running backup line should have an empty duration: %q", running)
	}
}
