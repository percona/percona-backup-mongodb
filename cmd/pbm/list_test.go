package main

import (
	"reflect"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
)

func Test_splitByBaseSnapshot(t *testing.T) {
	tl := oplog.Timeline{Start: 3, End: 7}

	t.Run("lastWrite is nil", func(t *testing.T) {
		lastWrite := bson.Timestamp{}
		got := splitByBaseSnapshot(lastWrite, tl)

		want := []pitrRange{
			{Range: tl, NoBaseSnapshot: true},
		}

		check(t, got, want)
	})

	t.Run("lastWrite > tl.End", func(t *testing.T) {
		lastWrite := bson.Timestamp{T: tl.End + 1}
		got := splitByBaseSnapshot(lastWrite, tl)

		want := []pitrRange{
			{Range: tl, NoBaseSnapshot: true},
		}

		check(t, got, want)
	})

	t.Run("lastWrite = tl.End", func(t *testing.T) {
		lastWrite := bson.Timestamp{T: tl.End}
		got := splitByBaseSnapshot(lastWrite, tl)

		want := []pitrRange{
			{Range: tl, NoBaseSnapshot: true},
		}

		check(t, got, want)
	})

	t.Run("lastWrite < tl.Start", func(t *testing.T) {
		lastWrite := bson.Timestamp{T: tl.Start - 1}
		got := splitByBaseSnapshot(lastWrite, tl)

		want := []pitrRange{
			{Range: tl, NoBaseSnapshot: true},
		}

		check(t, got, want)
	})

	t.Run("lastWrite = tl.Start", func(t *testing.T) {
		lastWrite := bson.Timestamp{T: tl.Start}
		got := splitByBaseSnapshot(lastWrite, tl)

		want := []pitrRange{
			{
				Range: oplog.Timeline{
					Start: lastWrite.T + 1,
					End:   tl.End,
				},
				NoBaseSnapshot: false,
			},
		}

		check(t, got, want)
	})

	t.Run("tl.Start < lastWrite < tl.End", func(t *testing.T) {
		lastWrite := bson.Timestamp{T: 5}
		got := splitByBaseSnapshot(lastWrite, tl)

		want := []pitrRange{
			{
				Range: oplog.Timeline{
					Start: tl.Start,
					End:   lastWrite.T,
				},
				NoBaseSnapshot: true,
			},
			{
				Range: oplog.Timeline{
					Start: lastWrite.T + 1,
					End:   tl.End,
				},
				NoBaseSnapshot: false,
			},
		}

		check(t, got, want)
	})
}

func check(t *testing.T, got, want []pitrRange) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got: %v, want: %v", got, want)
	}
}

func TestBackupListOutDuration(t *testing.T) {
	// 1755000150: 2025-08-12T12:02:30Z
	const restoreTS = int64(1755000150)

	bl := backupListOut{
		Snapshots: []snapshotStat{
			{
				Name:      "2025-08-12T12:00:00Z",
				Status:    defs.StatusDone,
				Type:      defs.LogicalBackup,
				RestoreTS: restoreTS,
				Duration:  "2m30s",
			},
			{
				Name:      "2025-08-12T13:00:00Z",
				Status:    defs.StatusDone,
				Type:      defs.PhysicalBackup,
				RestoreTS: restoreTS + 3600,
			},
		},
	}

	var hdr, withDur, withoutDur string
	for _, l := range strings.Split(bl.String(), "\n") {
		switch {
		case strings.Contains(l, "RESTORE TIME"):
			hdr = l
		case strings.Contains(l, "2025-08-12T12:00:00Z"):
			withDur = l
		case strings.Contains(l, "2025-08-12T13:00:00Z"):
			withoutDur = l
		}
	}

	if !strings.Contains(hdr, "DURATION") {
		t.Errorf("header is missing the DURATION column: %q", hdr)
	}
	if !strings.HasSuffix(strings.TrimRight(withDur, " "), "2m30s") {
		t.Errorf("backup line is missing the duration: %q", withDur)
	}
	if !strings.HasSuffix(strings.TrimRight(withoutDur, " "), "-") {
		t.Errorf("backup line without duration should end with a dash: %q", withoutDur)
	}
}
