package lifecycle

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	storagefs "github.com/percona/percona-backup-mongodb/pbm/storage/fs"
)

func evaluate(cfg config.LifecycleConf, backups []backup.BackupMeta, now time.Time) *Report {
	report := evaluateRetentionPolicy(cfg, backups, now)
	report.applyMinKeepGuard(backups)
	return report
}

// mockBcp is a helper to generate fake backups.
// daysAgo subtracts from the baseTime (our frozen mockNow).
func mockBcp(name string, daysAgo int, baseTime time.Time, status defs.Status) backup.BackupMeta {
	bcpTime := baseTime.AddDate(0, 0, -daysAgo)
	if daysAgo == 0 {
		bcpTime = bcpTime.Add(-time.Second)
	}
	return backup.BackupMeta{
		Name:    name,
		StartTS: bcpTime.Unix(),
		Status:  status,
	}
}

// mockTypedBcp is a helper to generate fake backups with a specific Type.
func mockTypedBcp(
	name string,
	daysAgo int,
	baseTime time.Time,
	status defs.Status,
	bcpType defs.BackupType,
) backup.BackupMeta {
	bcp := mockBcp(name, daysAgo, baseTime, status)
	bcp.Type = bcpType
	return bcp
}

func mockIncrementalBcp(
	name string,
	source string,
	daysAgo int,
	baseTime time.Time,
	status defs.Status,
) backup.BackupMeta {
	bcp := mockTypedBcp(name, daysAgo, baseTime, status, defs.IncrementalBackup)
	bcp.SrcBackup = source
	bcp.Store.StorageConf = config.StorageConf{
		Type:       storage.Filesystem,
		Filesystem: &storagefs.Config{Path: "/backups"},
	}
	return bcp
}

func withLastWrite(bcp backup.BackupMeta, ts uint32) backup.BackupMeta {
	bcp.LastWriteTS = bson.Timestamp{T: ts}
	return bcp
}

func TestFilterBackupsByProfile(t *testing.T) {
	backups := []backup.BackupMeta{
		{Name: "main"},
		{Name: "named-main", Store: backup.Storage{Name: "archive"}},
		{Name: "archive", Store: backup.Storage{Name: "archive", IsProfile: true}},
		{Name: "other", Store: backup.Storage{Name: "other", IsProfile: true}},
		{Name: "unnamed-profile", Store: backup.Storage{IsProfile: true}},
	}

	tests := []struct {
		name    string
		profile string
		want    []string
	}{
		{
			name: "main storage",
			want: []string{"main", "named-main"},
		},
		{
			name:    "named profile",
			profile: "archive",
			want:    []string{"archive"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterBackupsByProfile(backups, tt.profile)
			gotNames := make([]string, len(got))
			for i := range got {
				gotNames[i] = got[i].Name
			}

			if !reflect.DeepEqual(gotNames, tt.want) {
				t.Errorf("filterBackupsByProfile() = %v, want %v", gotNames, tt.want)
			}
		})
	}
}

func TestIsMainPITRBaseSnapshot(t *testing.T) {
	base := backup.BackupMeta{Type: defs.LogicalBackup, Status: defs.StatusDone}
	profile := base
	profile.Store.IsProfile = true
	failed := base
	failed.Status = defs.StatusError
	external := base
	external.Type = defs.ExternalBackup
	selective := base
	selective.Namespaces = []string{"db.collection"}

	tests := []struct {
		name string
		bcp  backup.BackupMeta
		want bool
	}{
		{name: "main", bcp: base, want: true},
		{name: "profile", bcp: profile},
		{name: "failed", bcp: failed},
		{name: "external", bcp: external},
		{name: "selective", bcp: selective},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isMainPITRBaseSnapshot(tt.bcp); got != tt.want {
				t.Fatalf("isMainPITRBaseSnapshot() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvaluate(t *testing.T) {
	// Define a few specific dates we want to test against
	standardDate := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)     // A normal Thursday
	leapYearDate := time.Date(2024, time.February, 29, 12, 0, 0, 0, time.UTC)  // Leap Day
	endOfYearDate := time.Date(2025, time.December, 31, 12, 0, 0, 0, time.UTC) // Dec 31st
	minKeepDisabled := 0

	tests := []struct {
		name           string
		cfg            config.LifecycleConf
		backups        []backup.BackupMeta
		mockNow        time.Time
		expectedKept   []string
		expectedPurged []string
	}{
		// --- STANDARD DATE SCENARIOS ---
		{
			name: "Feature Disabled",
			cfg: config.LifecycleConf{
				Enabled:        false,
				DailyRetention: 1,
			},
			mockNow: standardDate,
			backups: []backup.BackupMeta{
				mockBcp("bcp-today", 0, standardDate, defs.StatusDone),
				mockBcp("bcp-old", 10, standardDate, defs.StatusDone),
			},
			expectedKept:   []string{"bcp-today", "bcp-old"},
			expectedPurged: []string{},
		},
		{
			name: "Rolling Strategy - Basic GFS (7 Daily, 4 Weekly)",
			cfg: config.LifecycleConf{
				Enabled:         true,
				Strategy:        "rolling",
				DailyRetention:  7,
				WeeklyRetention: 4,
			},
			mockNow: standardDate,
			backups: []backup.BackupMeta{
				mockBcp("bcp-today", 0, standardDate, defs.StatusDone),
				mockBcp("bcp-7-days", 7, standardDate, defs.StatusDone),
				mockBcp("bcp-9-days", 9, standardDate, defs.StatusDone),
				mockBcp("bcp-12-days", 12, standardDate, defs.StatusDone), // Oldest in Week 1 bucket (Purged)
			},
			expectedKept:   []string{"bcp-today", "bcp-7-days"},
			expectedPurged: []string{"bcp-9-days", "bcp-12-days"},
		},
		{
			name: "Calendar Strategy - Exact match vs Nearest Neighbor",
			cfg: config.LifecycleConf{
				Enabled:          true,
				Strategy:         "calendar",
				DailyRetention:   0,
				MonthlyRetention: 1,
				MonthlyDay:       15, // Target the 15th
			},
			mockNow: standardDate, // March 26
			backups: []backup.BackupMeta{
				mockBcp("bcp-mar-13", 13, standardDate, defs.StatusDone), // 13 days ago (Mar 13, diff 2)
				mockBcp("bcp-mar-15", 11, standardDate, defs.StatusDone), // 11 days ago (Mar 15, Exact Match!)
			},
			expectedKept:   []string{"bcp-mar-15"},
			expectedPurged: []string{"bcp-mar-13"},
		},

		// --- LEAP YEAR SCENARIOS ---
		{
			name: "Leap Year - Daily Retention over Feb 29",
			cfg: config.LifecycleConf{
				Enabled:        true,
				Strategy:       "rolling",
				DailyRetention: 3,
			},
			mockNow: leapYearDate, // Feb 29, 2024
			backups: []backup.BackupMeta{
				mockBcp("bcp-feb-29", 0, leapYearDate, defs.StatusDone),
				mockBcp("bcp-feb-28", 1, leapYearDate, defs.StatusDone),
				mockBcp("bcp-feb-27", 2, leapYearDate, defs.StatusDone),
				mockBcp("bcp-feb-25", 4, leapYearDate, defs.StatusDone), // 4 days ago, Purged
			},
			expectedKept:   []string{"bcp-feb-29", "bcp-feb-28", "bcp-feb-27"},
			expectedPurged: []string{"bcp-feb-25"},
		},

		// --- END OF YEAR SCENARIOS ---
		{
			name: "End of Year - Monthly Nearest Neighbor across year boundary",
			cfg: config.LifecycleConf{
				Enabled:          true,
				Strategy:         "calendar",
				DailyRetention:   0,
				MonthlyRetention: 1,
				MonthlyDay:       1, // Target the 1st of the month
			},
			mockNow: endOfYearDate, // Dec 31, 2025
			backups: []backup.BackupMeta{
				mockBcp("bcp-dec-2", 29, endOfYearDate, defs.StatusDone),  // Dec 2 (Diff 1)
				mockBcp("bcp-nov-29", 32, endOfYearDate, defs.StatusDone), // Nov 29 (Diff 2)
			},
			expectedKept:   []string{"bcp-dec-2"},
			expectedPurged: []string{"bcp-nov-29"},
		},

		// --- STATE HANDLING SCENARIOS ---
		{
			name: "In-Progress Backups are protected without changing policy decisions",
			cfg: config.LifecycleConf{
				Enabled:        true,
				DailyRetention: 1,
				MinKeep:        &minKeepDisabled,
			},
			mockNow: standardDate,
			backups: []backup.BackupMeta{
				mockBcp("bcp-running", 0, standardDate, defs.StatusRunning), // In-progress today
				mockBcp("bcp-done-old", 50, standardDate, defs.StatusDone),  // 50 days old, normally purged
			},
			// bcp-running is implicitly protected (hidden from purge).
			expectedKept:   []string{},
			expectedPurged: []string{"bcp-done-old"},
		},
		{
			name: "Failed Backups - PurgeFailed is TRUE",
			cfg: config.LifecycleConf{
				Enabled:        true,
				PurgeFailed:    true,
				DailyRetention: 7, // Protect for 7 days
				MinKeep:        &minKeepDisabled,
			},
			mockNow: standardDate,
			backups: []backup.BackupMeta{
				mockBcp("bcp-error-recent", 3, standardDate, defs.StatusError), // Kept
				mockBcp("bcp-error-old", 10, standardDate, defs.StatusError),   // Purged
			},
			expectedKept:   []string{"bcp-error-recent"},
			expectedPurged: []string{"bcp-error-old"},
		},
		{
			name: "Type-Aware Bucketing - Keeps both Physical and Logical for the same week",
			cfg: config.LifecycleConf{
				Enabled:         true,
				Strategy:        "rolling",
				DailyRetention:  0, // Disable daily to force Weekly evaluation
				WeeklyRetention: 2,
			},
			mockNow: standardDate,
			backups: []backup.BackupMeta{
				// Week 1 Bucket (7-13 days ago)
				mockTypedBcp("phys-newer", 9, standardDate, defs.StatusDone, defs.PhysicalBackup),
				mockTypedBcp("phys-older", 12, standardDate, defs.StatusDone, defs.PhysicalBackup), // Loses to phys-newer

				// Same Week 1 Bucket, but Logical
				mockTypedBcp("logical-only", 11, standardDate, defs.StatusDone, defs.LogicalBackup), // Wins its own logical bucket
			},
			expectedKept:   []string{"phys-newer", "logical-only"},
			expectedPurged: []string{"phys-older"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := evaluate(tt.cfg, tt.backups, tt.mockNow)

			if report.Aborted {
				t.Errorf("unexpected aborted plan: %s", report.AbortReason)
			}

			sort.Strings(report.BackupsKept)
			sort.Strings(tt.expectedKept)
			sort.Strings(report.BackupsPurged)
			sort.Strings(tt.expectedPurged)

			if len(report.BackupsKept) == 0 && len(tt.expectedKept) == 0 {
				// both empty, pass
			} else if !reflect.DeepEqual(report.BackupsKept, tt.expectedKept) {
				t.Errorf("BackupsKept = %v, want %v", report.BackupsKept, tt.expectedKept)
			}

			if len(report.BackupsPurged) == 0 && len(tt.expectedPurged) == 0 {
				// both empty, pass
			} else if !reflect.DeepEqual(report.BackupsPurged, tt.expectedPurged) {
				t.Errorf("BackupsPurged = %v, want %v", report.BackupsPurged, tt.expectedPurged)
			}
		})
	}
}

func TestEvaluateRollingBucketBoundaries(t *testing.T) {
	now := time.Date(2026, time.March, 18, 12, 0, 0, 0, time.UTC)
	minKeep := 0

	t.Run("weekly", func(t *testing.T) {
		report := evaluate(config.LifecycleConf{
			Enabled:         true,
			MinKeep:         &minKeep,
			WeeklyRetention: 2,
		}, []backup.BackupMeta{
			mockTypedBcp("day-0", 0, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("day-6", 6, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("day-7", 7, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("day-13", 13, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("day-14", 14, now, defs.StatusDone, defs.LogicalBackup),
		}, now)

		if !reflect.DeepEqual(report.BackupsKept, []string{"day-0", "day-7"}) {
			t.Fatalf("BackupsKept = %v, want day-0 and day-7", report.BackupsKept)
		}
		if !reflect.DeepEqual(report.BackupsPurged, []string{"day-6", "day-13", "day-14"}) {
			t.Fatalf("BackupsPurged = %v, want exact two-bucket boundary", report.BackupsPurged)
		}
	})

	t.Run("monthly", func(t *testing.T) {
		report := evaluate(config.LifecycleConf{
			Enabled:          true,
			MinKeep:          &minKeep,
			MonthlyRetention: 2,
		}, []backup.BackupMeta{
			mockTypedBcp("day-0", 0, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("day-29", 29, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("day-30", 30, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("day-59", 59, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("day-60", 60, now, defs.StatusDone, defs.LogicalBackup),
		}, now)

		if !reflect.DeepEqual(report.BackupsKept, []string{"day-0", "day-30"}) {
			t.Fatalf("BackupsKept = %v, want day-0 and day-30", report.BackupsKept)
		}
		if !reflect.DeepEqual(report.BackupsPurged, []string{"day-29", "day-59", "day-60"}) {
			t.Fatalf("BackupsPurged = %v, want exact two-bucket boundary", report.BackupsPurged)
		}
	})
}

func TestEvaluateCalendarBucketsIncludeCurrentPeriod(t *testing.T) {
	minKeep := 0

	t.Run("weekly", func(t *testing.T) {
		now := time.Date(2026, time.March, 18, 12, 0, 0, 0, time.UTC) // Wednesday
		report := evaluate(config.LifecycleConf{
			Enabled:         true,
			Strategy:        config.LifecycleStrategyCalendar,
			MinKeep:         &minKeep,
			WeeklyRetention: 2,
			WeeklyDay:       int(time.Friday),
		}, []backup.BackupMeta{
			mockTypedBcp("current-wednesday", 0, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("current-monday", 2, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("previous-friday", 5, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("previous-monday", 9, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("two-weeks-friday", 12, now, defs.StatusDone, defs.LogicalBackup),
		}, now)

		if !reflect.DeepEqual(report.BackupsKept, []string{"current-wednesday", "previous-friday"}) {
			t.Fatalf("BackupsKept = %v, want current and previous calendar weeks", report.BackupsKept)
		}
	})

	t.Run("monthly", func(t *testing.T) {
		now := time.Date(2026, time.March, 18, 12, 0, 0, 0, time.UTC)
		report := evaluate(config.LifecycleConf{
			Enabled:          true,
			Strategy:         config.LifecycleStrategyCalendar,
			MinKeep:          &minKeep,
			MonthlyRetention: 2,
			MonthlyDay:       15,
		}, []backup.BackupMeta{
			mockTypedBcp("march-15", 3, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("february-15", 31, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("january-15", 62, now, defs.StatusDone, defs.LogicalBackup),
		}, now)

		if !reflect.DeepEqual(report.BackupsKept, []string{"march-15", "february-15"}) {
			t.Fatalf("BackupsKept = %v, want current and previous calendar months", report.BackupsKept)
		}
	})

	t.Run("sunday target", func(t *testing.T) {
		now := time.Date(2026, time.March, 22, 12, 0, 0, 0, time.UTC) // Sunday
		report := evaluate(config.LifecycleConf{
			Enabled:         true,
			Strategy:        config.LifecycleStrategyCalendar,
			MinKeep:         &minKeep,
			WeeklyRetention: 1,
			WeeklyDay:       int(time.Sunday),
		}, []backup.BackupMeta{
			mockTypedBcp("monday", 6, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("saturday", 1, now, defs.StatusDone, defs.LogicalBackup),
		}, now)

		if !reflect.DeepEqual(report.BackupsKept, []string{"saturday"}) {
			t.Fatalf("BackupsKept = %v, want Saturday as nearest Sunday neighbor", report.BackupsKept)
		}
	})

	t.Run("equal distance prefers newest", func(t *testing.T) {
		now := time.Date(2026, time.March, 18, 12, 0, 0, 0, time.UTC)
		report := evaluate(config.LifecycleConf{
			Enabled:          true,
			Strategy:         config.LifecycleStrategyCalendar,
			MinKeep:          &minKeep,
			MonthlyRetention: 1,
			MonthlyDay:       15,
		}, []backup.BackupMeta{
			mockTypedBcp("march-14", 4, now, defs.StatusDone, defs.LogicalBackup),
			mockTypedBcp("march-16", 2, now, defs.StatusDone, defs.LogicalBackup),
		}, now)

		if !reflect.DeepEqual(report.BackupsKept, []string{"march-16"}) {
			t.Fatalf("BackupsKept = %v, want newest equal-distance candidate", report.BackupsKept)
		}
	})
}

func TestBetterCandidate(t *testing.T) {
	current := backup.BackupMeta{
		Name:        "current",
		StartTS:     100,
		LastWriteTS: bson.Timestamp{T: 200, I: 1},
	}
	tests := []struct {
		name      string
		candidate backup.BackupMeta
		current   *backup.BackupMeta
		want      bool
	}{
		{
			name:      "nil current",
			candidate: backup.BackupMeta{},
			want:      true,
		},
		{
			name: "newer restore time",
			candidate: backup.BackupMeta{
				StartTS:     99,
				LastWriteTS: bson.Timestamp{T: 201},
			},
			current: &current,
			want:    true,
		},
		{
			name: "older restore time",
			candidate: backup.BackupMeta{
				StartTS:     101,
				LastWriteTS: bson.Timestamp{T: 199},
			},
			current: &current,
		},
		{
			name: "newer restore increment",
			candidate: backup.BackupMeta{
				StartTS:     99,
				LastWriteTS: bson.Timestamp{T: 200, I: 2},
			},
			current: &current,
			want:    true,
		},
		{
			name: "equal restore time newer start",
			candidate: backup.BackupMeta{
				StartTS:     101,
				LastWriteTS: current.LastWriteTS,
			},
			current: &current,
			want:    true,
		},
		{
			name: "equal restore time older start",
			candidate: backup.BackupMeta{
				StartTS:     99,
				LastWriteTS: current.LastWriteTS,
			},
			current: &current,
		},
		{
			name: "equal times smaller name",
			candidate: backup.BackupMeta{
				Name:        "alpha",
				StartTS:     current.StartTS,
				LastWriteTS: current.LastWriteTS,
			},
			current: &current,
			want:    true,
		},
		{
			name: "equal times larger name",
			candidate: backup.BackupMeta{
				Name:        "zeta",
				StartTS:     current.StartTS,
				LastWriteTS: current.LastWriteTS,
			},
			current: &current,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := betterCandidate(&tt.candidate, tt.current); got != tt.want {
				t.Fatalf("betterCandidate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvaluateOverlappingRetentionReasons(t *testing.T) {
	now := time.Date(2026, time.March, 15, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	report := evaluate(config.LifecycleConf{
		Enabled:          true,
		Strategy:         config.LifecycleStrategyCalendar,
		MinKeep:          &minKeep,
		DailyRetention:   7,
		WeeklyRetention:  1,
		WeeklyDay:        int(now.Weekday()),
		MonthlyRetention: 1,
		MonthlyDay:       now.Day(),
	}, []backup.BackupMeta{
		mockTypedBcp("anchor", 0, now, defs.StatusDone, defs.LogicalBackup),
	}, now)

	want := []string{"Daily", "Weekly", "Monthly"}
	if !reflect.DeepEqual(report.KeepReasons["anchor"], want) {
		t.Fatalf("KeepReasons = %v, want %v", report.KeepReasons["anchor"], want)
	}
}

func TestEvaluateExcludesSelectiveAndManagesExternal(t *testing.T) {
	now := time.Date(2026, time.March, 18, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	selectiveA := mockTypedBcp("selective-a", 0, now, defs.StatusDone, defs.LogicalBackup)
	selectiveA.Namespaces = []string{"db.a"}
	selectiveB := mockTypedBcp("selective-b", 1, now, defs.StatusDone, defs.LogicalBackup)
	selectiveB.Namespaces = []string{"db.b"}
	failedSelective := mockTypedBcp("selective-failed", 20, now, defs.StatusError, defs.LogicalBackup)
	failedSelective.Namespaces = []string{"db.failed"}
	canceledSelective := mockTypedBcp("selective-canceled", 20, now, defs.StatusCancelled, defs.LogicalBackup)
	canceledSelective.Namespaces = []string{"db.canceled"}
	runningSelective := mockTypedBcp("selective-running", 0, now, defs.StatusRunning, defs.LogicalBackup)
	runningSelective.Namespaces = []string{"db.running"}

	report := evaluate(config.LifecycleConf{
		Enabled:         true,
		PurgeFailed:     true,
		MinKeep:         &minKeep,
		WeeklyRetention: 1,
	}, []backup.BackupMeta{
		selectiveA,
		selectiveB,
		failedSelective,
		canceledSelective,
		runningSelective,
		mockTypedBcp("full-logical", 2, now, defs.StatusDone, defs.LogicalBackup),
		mockTypedBcp("expired-logical", 8, now, defs.StatusDone, defs.LogicalBackup),
		mockTypedBcp("external", 2, now, defs.StatusDone, defs.ExternalBackup),
		mockTypedBcp("expired-external", 8, now, defs.StatusDone, defs.ExternalBackup),
	}, now)

	for _, name := range []string{"selective-a", "selective-b", "selective-failed", "selective-canceled"} {
		if !reflect.DeepEqual(report.KeepReasons[name], []string{selectiveBackupReason}) {
			t.Errorf("%s reasons = %v, want selective exclusion", name, report.KeepReasons[name])
		}
	}
	if !reflect.DeepEqual(report.KeepReasons["full-logical"], []string{"Weekly"}) {
		t.Fatalf("full logical backup was displaced: %v", report.KeepReasons)
	}
	if !reflect.DeepEqual(report.KeepReasons["external"], []string{"Weekly"}) {
		t.Fatalf("External backup should remain lifecycle-managed: %v", report.KeepReasons)
	}
	if !reflect.DeepEqual(report.BackupsPurged, []string{"expired-logical", "expired-external"}) {
		t.Fatalf("BackupsPurged = %v, want expired managed backups", report.BackupsPurged)
	}
	if _, ok := report.BackupTypes["selective-running"]; ok {
		t.Fatal("running selective backup appeared in lifecycle report")
	}
}

func TestEvaluateProtectsBackupsCreatedAfterEvaluation(t *testing.T) {
	now := time.Date(2026, time.March, 18, 12, 0, 0, 500_000_000, time.UTC)
	minKeep := 0
	sameSecond := mockTypedBcp("same-second", 0, now, defs.StatusDone, defs.LogicalBackup)
	sameSecond.StartTS = now.Unix()
	future := mockTypedBcp("future", -1, now, defs.StatusDone, defs.LogicalBackup)
	report := evaluate(config.LifecycleConf{
		Enabled: true,
		MinKeep: &minKeep,
	}, []backup.BackupMeta{sameSecond, future}, now)

	for _, name := range []string{"same-second", "future"} {
		if !reflect.DeepEqual(report.KeepReasons[name], []string{afterEvaluationReason}) {
			t.Errorf("%s backup reasons = %v, want evaluation-time protection", name, report.KeepReasons[name])
		}
	}
	if len(report.DeleteTargets) != 0 {
		t.Fatalf("evaluation-time-protected backup became a deletion target: %v", report.DeleteTargets)
	}
}

func TestEvaluateExcludesInProgressStatuses(t *testing.T) {
	now := time.Date(2026, time.March, 18, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	report := evaluate(config.LifecycleConf{
		Enabled: true,
		MinKeep: &minKeep,
	}, []backup.BackupMeta{
		mockTypedBcp("starting", 10, now, defs.StatusStarting, defs.LogicalBackup),
		mockTypedBcp("running", 10, now, defs.StatusRunning, defs.LogicalBackup),
		mockTypedBcp("dump-done", 10, now, defs.StatusDumpDone, defs.LogicalBackup),
		mockTypedBcp("done", 10, now, defs.StatusDone, defs.LogicalBackup),
	}, now)

	if !reflect.DeepEqual(report.BackupsPurged, []string{"done"}) {
		t.Fatalf("BackupsPurged = %v, want only completed backup", report.BackupsPurged)
	}
	for _, name := range []string{"starting", "running", "dump-done"} {
		if _, ok := report.BackupTypes[name]; ok {
			t.Errorf("in-progress backup %q appeared in lifecycle report", name)
		}
	}
}

func TestEvaluateRetainsCompleteIncrementalChain(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	report := evaluate(config.LifecycleConf{
		Enabled:        true,
		DailyRetention: 3,
		MinKeep:        &minKeep,
	}, []backup.BackupMeta{
		mockIncrementalBcp("inc-2", "inc-1", 1, now, defs.StatusDone),
		mockIncrementalBcp("inc-1", "base", 10, now, defs.StatusDone),
		mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
	}, now)

	wantKept := []string{"inc-2", "inc-1", "base"}
	if !reflect.DeepEqual(report.BackupsKept, wantKept) {
		t.Fatalf("BackupsKept = %v, want %v", report.BackupsKept, wantKept)
	}
	if len(report.BackupsPurged) != 0 {
		t.Fatalf("BackupsPurged = %v, want none", report.BackupsPurged)
	}
	if len(report.DeleteTargets) != 0 {
		t.Fatalf("DeleteTargets = %v, want none", report.DeleteTargets)
	}
	if !reflect.DeepEqual(report.KeepReasons["inc-2"], []string{"Daily"}) {
		t.Errorf("inc-2 reasons = %v, want Daily", report.KeepReasons["inc-2"])
	}
	for _, name := range []string{"inc-1", "base"} {
		if !reflect.DeepEqual(report.KeepReasons[name], []string{incrementalChainReason}) {
			t.Errorf("%s reasons = %v, want %q", name, report.KeepReasons[name], incrementalChainReason)
		}
	}
}

func TestEvaluateRetainedBaseKeepsNewerIncrements(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	report := evaluate(config.LifecycleConf{
		Enabled:         true,
		Strategy:        "rolling",
		WeeklyRetention: 6,
		MinKeep:         &minKeep,
	}, []backup.BackupMeta{
		mockIncrementalBcp("other-inc", "other-base", 8, now, defs.StatusDone),
		mockIncrementalBcp("inc-2", "inc-1", 9, now, defs.StatusDone),
		mockIncrementalBcp("inc-1", "base", 10, now, defs.StatusDone),
		mockIncrementalBcp("other-base", "", 20, now, defs.StatusDone),
		mockIncrementalBcp("base", "", 35, now, defs.StatusDone),
	}, now)

	wantKept := []string{"other-inc", "inc-2", "inc-1", "other-base", "base"}
	if !reflect.DeepEqual(report.BackupsKept, wantKept) {
		t.Fatalf("BackupsKept = %v, want %v", report.BackupsKept, wantKept)
	}
	if !reflect.DeepEqual(report.KeepReasons["base"], []string{"Weekly"}) {
		t.Errorf("base reasons = %v, want Weekly", report.KeepReasons["base"])
	}
	for _, name := range []string{"inc-1", "inc-2"} {
		if !reflect.DeepEqual(report.KeepReasons[name], []string{incrementalChainReason}) {
			t.Errorf("%s reasons = %v, want %q", name, report.KeepReasons[name], incrementalChainReason)
		}
	}
	if len(report.DeleteTargets) != 0 {
		t.Fatalf("DeleteTargets = %v, want none", report.DeleteTargets)
	}
}

func TestEvaluatePurgesIncrementalChainThroughRoot(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	report := evaluate(config.LifecycleConf{
		Enabled:     true,
		PurgeFailed: true,
		MinKeep:     &minKeep,
	}, []backup.BackupMeta{
		mockIncrementalBcp("inc-1", "base", 10, now, defs.StatusDone),
		mockIncrementalBcp("failed-attempt", "base", 12, now, defs.StatusError),
		mockTypedBcp("logical", 20, now, defs.StatusDone, defs.LogicalBackup),
		mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
	}, now)

	wantPurged := []string{"inc-1", "failed-attempt", "logical", "base"}
	if !reflect.DeepEqual(report.BackupsPurged, wantPurged) {
		t.Fatalf("BackupsPurged = %v, want %v", report.BackupsPurged, wantPurged)
	}
	wantTargets := []string{"logical", "base"}
	if !reflect.DeepEqual(report.DeleteTargets, wantTargets) {
		t.Fatalf("DeleteTargets = %v, want %v", report.DeleteTargets, wantTargets)
	}
}

func TestPITRBaseCandidates(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	cfg := config.LifecycleConf{
		Enabled:        true,
		DailyRetention: 1,
		MinKeep:        &minKeep,
	}

	t.Run("multiple purge candidates", func(t *testing.T) {
		backups := []backup.BackupMeta{
			withLastWrite(mockTypedBcp("survivor", 0, now, defs.StatusDone, defs.LogicalBackup), 100),
			withLastWrite(mockTypedBcp("latest", 10, now, defs.StatusDone, defs.LogicalBackup), 300),
			withLastWrite(mockTypedBcp("older", 20, now, defs.StatusDone, defs.LogicalBackup), 200),
		}
		report := evaluate(cfg, backups, now)

		candidates, err := report.findPITRBaseCandidates(backups)
		if err != nil {
			t.Fatal(err)
		}
		if len(candidates.anchors) == 0 {
			t.Fatal("PITR base candidate not found")
		}
		if !reflect.DeepEqual(candidates.anchors, []string{"latest"}) ||
			candidates.previousRestoreTime.T != 100 || candidates.restoreTime.T != 300 {
			t.Fatalf("candidates = %+v, want latest with range 100-300", candidates)
		}
		if !isRequiredPITRBase(candidates.previousRestoreTime, []oplog.Timeline{{Start: 150, End: 400}}) {
			t.Fatal("candidate should be required for PITR")
		}
		report.protectPITRAnchors(candidates.anchors, backups)

		if !reflect.DeepEqual(report.BackupsKept, []string{"survivor", "latest"}) {
			t.Fatalf("BackupsKept = %v, want survivor and latest", report.BackupsKept)
		}
		if !reflect.DeepEqual(report.BackupsPurged, []string{"older"}) {
			t.Fatalf("BackupsPurged = %v, want older", report.BackupsPurged)
		}
		if !reflect.DeepEqual(report.DeleteTargets, []string{"older"}) {
			t.Fatalf("DeleteTargets = %v, want older", report.DeleteTargets)
		}
		if !reflect.DeepEqual(report.KeepReasons["latest"], []string{pitrBaseSnapshotReason}) {
			t.Errorf("latest reasons = %v, want %q", report.KeepReasons["latest"], pitrBaseSnapshotReason)
		}
	})

	t.Run("newer survivor", func(t *testing.T) {
		backups := []backup.BackupMeta{
			withLastWrite(mockTypedBcp("survivor", 0, now, defs.StatusDone, defs.LogicalBackup), 400),
			withLastWrite(mockTypedBcp("candidate", 10, now, defs.StatusDone, defs.LogicalBackup), 300),
		}
		report := evaluate(cfg, backups, now)

		candidates, err := report.findPITRBaseCandidates(backups)
		if err != nil {
			t.Fatal(err)
		}
		if len(candidates.anchors) != 0 {
			t.Fatal("PITR base candidate found despite newer surviving snapshot")
		}
		if !reflect.DeepEqual(report.BackupsPurged, []string{"candidate"}) {
			t.Fatalf("BackupsPurged = %v, want candidate", report.BackupsPurged)
		}
		if !reflect.DeepEqual(report.DeleteTargets, []string{"candidate"}) {
			t.Fatalf("DeleteTargets = %v, want candidate", report.DeleteTargets)
		}
	})

	t.Run("multi-increment chain", func(t *testing.T) {
		backups := []backup.BackupMeta{
			withLastWrite(mockTypedBcp("survivor", 0, now, defs.StatusDone, defs.LogicalBackup), 100),
			withLastWrite(mockIncrementalBcp("inc-2", "inc-1", 10, now, defs.StatusDone), 400),
			withLastWrite(mockIncrementalBcp("inc-1", "base", 20, now, defs.StatusDone), 300),
			withLastWrite(mockIncrementalBcp("base", "", 30, now, defs.StatusDone), 200),
		}
		report := evaluate(cfg, backups, now)

		candidates, err := report.findPITRBaseCandidates(backups)
		if err != nil {
			t.Fatal(err)
		}
		if len(candidates.anchors) == 0 {
			t.Fatal("PITR base candidate not found")
		}
		if !reflect.DeepEqual(candidates.anchors, []string{"inc-2"}) ||
			candidates.previousRestoreTime.T != 100 || candidates.restoreTime.T != 400 {
			t.Fatalf("candidates = %+v, want inc-2 with projected range 100-400", candidates)
		}
		if !isRequiredPITRBase(candidates.previousRestoreTime, []oplog.Timeline{{Start: 350, End: 500}}) {
			t.Fatal("incremental chain should be required for PITR")
		}
		report.protectPITRAnchors(candidates.anchors, backups)

		wantKept := []string{"survivor", "inc-2", "inc-1", "base"}
		if !reflect.DeepEqual(report.BackupsKept, wantKept) {
			t.Fatalf("BackupsKept = %v, want %v", report.BackupsKept, wantKept)
		}
		if len(report.BackupsPurged) != 0 || len(report.DeleteTargets) != 0 {
			t.Fatalf("PITR-protected chain remained purgeable: %+v", report)
		}
		if !reflect.DeepEqual(report.KeepReasons["inc-2"], []string{pitrBaseSnapshotReason}) {
			t.Errorf("inc-2 reasons = %v, want %q", report.KeepReasons["inc-2"], pitrBaseSnapshotReason)
		}
		for _, name := range []string{"base", "inc-1"} {
			if !reflect.DeepEqual(report.KeepReasons[name], []string{incrementalChainReason}) {
				t.Errorf("%s reasons = %v, want %q", name, report.KeepReasons[name], incrementalChainReason)
			}
		}
	})

	t.Run("incremental candidate uses projected survivor", func(t *testing.T) {
		backups := []backup.BackupMeta{
			withLastWrite(mockTypedBcp("survivor", 0, now, defs.StatusDone, defs.LogicalBackup), 50),
			withLastWrite(mockIncrementalBcp("inc-2", "inc-1", 10, now, defs.StatusDone), 300),
			withLastWrite(mockTypedBcp("standalone", 15, now, defs.StatusDone, defs.LogicalBackup), 250),
			withLastWrite(mockIncrementalBcp("inc-1", "base", 20, now, defs.StatusDone), 150),
			withLastWrite(mockIncrementalBcp("base", "", 30, now, defs.StatusDone), 100),
		}
		report := evaluate(cfg, backups, now)

		candidates, err := report.findPITRBaseCandidates(backups)
		if err != nil {
			t.Fatal(err)
		}
		if len(candidates.anchors) == 0 ||
			candidates.previousRestoreTime.T != 50 || candidates.restoreTime.T != 300 {
			t.Fatalf("candidates = %+v, want projected range 50-300", candidates)
		}
		if !isRequiredPITRBase(candidates.previousRestoreTime, []oplog.Timeline{{Start: 100, End: 300}}) {
			t.Fatal("purged chain members cannot replace the surviving PITR base")
		}
		report.protectPITRAnchors(candidates.anchors, backups)

		if !reflect.DeepEqual(report.BackupsPurged, []string{"standalone"}) {
			t.Fatalf("BackupsPurged = %v, want standalone", report.BackupsPurged)
		}
		if !reflect.DeepEqual(report.DeleteTargets, []string{"standalone"}) {
			t.Fatalf("DeleteTargets = %v, want standalone", report.DeleteTargets)
		}
	})

	t.Run("survivor at incremental root timestamp remains eligible", func(t *testing.T) {
		backups := []backup.BackupMeta{
			withLastWrite(mockTypedBcp("survivor", 0, now, defs.StatusDone, defs.LogicalBackup), 100),
			withLastWrite(mockIncrementalBcp("inc-1", "base", 10, now, defs.StatusDone), 300),
			withLastWrite(mockTypedBcp("standalone", 20, now, defs.StatusDone, defs.LogicalBackup), 200),
			withLastWrite(mockIncrementalBcp("base", "", 30, now, defs.StatusDone), 100),
		}
		report := evaluate(cfg, backups, now)

		candidates, err := report.findPITRBaseCandidates(backups)
		if err != nil {
			t.Fatal(err)
		}
		if len(candidates.anchors) == 0 ||
			candidates.previousRestoreTime.T != 100 || candidates.restoreTime.T != 300 {
			t.Fatalf("candidates = %+v, want projected range 100-300", candidates)
		}
		if isRequiredPITRBase(candidates.previousRestoreTime, []oplog.Timeline{{Start: 50, End: 300}}) {
			t.Fatal("surviving snapshot can replace the incremental chain")
		}
	})

	t.Run("equal latest restore timestamps", func(t *testing.T) {
		backups := []backup.BackupMeta{
			withLastWrite(mockTypedBcp("survivor", 0, now, defs.StatusDone, defs.LogicalBackup), 100),
			withLastWrite(mockTypedBcp("zeta", 10, now, defs.StatusDone, defs.LogicalBackup), 300),
			withLastWrite(mockTypedBcp("alpha", 20, now, defs.StatusDone, defs.LogicalBackup), 300),
		}
		report := evaluate(cfg, backups, now)

		candidates, err := report.findPITRBaseCandidates(backups)
		if err != nil {
			t.Fatal(err)
		}
		if len(candidates.anchors) == 0 {
			t.Fatal("PITR base candidates not found")
		}
		if !reflect.DeepEqual(candidates.anchors, []string{"alpha", "zeta"}) {
			t.Fatalf("anchors = %v, want deterministic ties", candidates.anchors)
		}
		if candidates.previousRestoreTime.T != 100 || candidates.restoreTime.T != 300 {
			t.Fatalf("candidates = %+v, want range 100-300", candidates)
		}
		if !isRequiredPITRBase(candidates.previousRestoreTime, []oplog.Timeline{{Start: 150, End: 400}}) {
			t.Fatal("tied candidates should be required for PITR")
		}
		report.protectPITRAnchors(candidates.anchors, backups)

		if len(report.BackupsPurged) != 0 || len(report.DeleteTargets) != 0 {
			t.Fatalf("tied PITR bases remained purgeable: %+v", report)
		}
		for _, name := range []string{"alpha", "zeta"} {
			if !reflect.DeepEqual(report.KeepReasons[name], []string{pitrBaseSnapshotReason}) {
				t.Errorf("%s reasons = %v, want %q", name, report.KeepReasons[name], pitrBaseSnapshotReason)
			}
		}
	})

	t.Run("ineligible newer backups", func(t *testing.T) {
		selective := withLastWrite(
			mockTypedBcp("selective", 20, now, defs.StatusDone, defs.LogicalBackup),
			500,
		)
		selective.Namespaces = []string{"db.collection"}
		backups := []backup.BackupMeta{
			withLastWrite(mockTypedBcp("survivor", 0, now, defs.StatusDone, defs.LogicalBackup), 100),
			withLastWrite(mockTypedBcp("candidate", 10, now, defs.StatusDone, defs.LogicalBackup), 300),
			selective,
			withLastWrite(mockTypedBcp("external", 30, now, defs.StatusDone, defs.ExternalBackup), 600),
		}
		report := evaluate(cfg, backups, now)

		candidates, err := report.findPITRBaseCandidates(backups)
		if err != nil {
			t.Fatal(err)
		}
		if len(candidates.anchors) == 0 || !reflect.DeepEqual(candidates.anchors, []string{"candidate"}) {
			t.Fatalf("candidates = %+v, want candidate despite newer ineligible backups", candidates)
		}
	})

	t.Run("no previous survivor", func(t *testing.T) {
		backups := []backup.BackupMeta{
			withLastWrite(mockTypedBcp("candidate", 10, now, defs.StatusDone, defs.LogicalBackup), 300),
		}
		report := evaluate(cfg, backups, now)

		candidates, err := report.findPITRBaseCandidates(backups)
		if err != nil {
			t.Fatal(err)
		}
		if len(candidates.anchors) == 0 || !candidates.previousRestoreTime.IsZero() {
			t.Fatalf("candidates = %+v, want no previous survivor", candidates)
		}
		if !isRequiredPITRBase(candidates.previousRestoreTime, []oplog.Timeline{{Start: 150, End: 400}}) {
			t.Fatal("candidate without a previous survivor should be required for PITR")
		}
	})

	t.Run("timeline starts at previous survivor", func(t *testing.T) {
		previousRestoreTime := bson.Timestamp{T: 150}
		if isRequiredPITRBase(previousRestoreTime, []oplog.Timeline{{Start: 150, End: 400}}) {
			t.Fatal("candidate should not be required when the previous survivor starts the timeline")
		}
	})

	t.Run("discontinuous timeline", func(t *testing.T) {
		backups := []backup.BackupMeta{
			withLastWrite(mockTypedBcp("survivor", 0, now, defs.StatusDone, defs.LogicalBackup), 100),
			withLastWrite(mockTypedBcp("candidate", 10, now, defs.StatusDone, defs.LogicalBackup), 300),
		}
		report := evaluate(cfg, backups, now)

		candidates, err := report.findPITRBaseCandidates(backups)
		if err != nil {
			t.Fatal(err)
		}
		if len(candidates.anchors) == 0 {
			t.Fatal("PITR base candidate not found")
		}
		timelines := []oplog.Timeline{{Start: 150, End: 200}, {Start: 250, End: 300}}
		if isRequiredPITRBase(candidates.previousRestoreTime, timelines) {
			t.Fatal("candidate should not be required across a discontinuous timeline")
		}
		if !reflect.DeepEqual(report.BackupsPurged, []string{"candidate"}) {
			t.Fatalf("BackupsPurged = %v, want candidate", report.BackupsPurged)
		}
	})

	t.Run("no timeline", func(t *testing.T) {
		if isRequiredPITRBase(bson.Timestamp{T: 100}, nil) {
			t.Fatal("candidate should not be required without a PITR timeline")
		}
	})
}

func TestSortDeleteTargetsByRestoreTime(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	backups := []backup.BackupMeta{
		withLastWrite(mockIncrementalBcp("inc-1", "base", 10, now, defs.StatusDone), 200),
		withLastWrite(mockTypedBcp("old", 20, now, defs.StatusDone, defs.LogicalBackup), 100),
		withLastWrite(mockTypedBcp("new", 30, now, defs.StatusDone, defs.LogicalBackup), 300),
		withLastWrite(mockIncrementalBcp("base", "", 40, now, defs.StatusDone), 150),
	}
	report := evaluate(config.LifecycleConf{
		Enabled: true,
		MinKeep: &minKeep,
	}, backups, now)

	report.sortDeleteTargets(backups)
	want := []string{"new", "base", "old"}
	if !reflect.DeepEqual(report.DeleteTargets, want) {
		t.Fatalf("DeleteTargets = %v, want newest-first %v", report.DeleteTargets, want)
	}
}

func TestEvaluateProtectedIncrementRetainsChain(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0

	t.Run("failed attempt", func(t *testing.T) {
		report := evaluate(config.LifecycleConf{
			Enabled: true,
			MinKeep: &minKeep,
		}, []backup.BackupMeta{
			mockIncrementalBcp("failed-attempt", "base", 10, now, defs.StatusError),
			mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
		}, now)

		wantKept := []string{"failed-attempt", "base"}
		if !reflect.DeepEqual(report.BackupsKept, wantKept) {
			t.Fatalf("BackupsKept = %v, want %v", report.BackupsKept, wantKept)
		}
		if len(report.DeleteTargets) != 0 {
			t.Fatalf("DeleteTargets = %v, want none", report.DeleteTargets)
		}
	})

	t.Run("running increment", func(t *testing.T) {
		backups := []backup.BackupMeta{
			mockIncrementalBcp("running", "base", 0, now, defs.StatusRunning),
			mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
		}
		report := evaluate(config.LifecycleConf{
			Enabled:     true,
			PurgeFailed: true,
			MinKeep:     &minKeep,
		}, backups, now)

		if !reflect.DeepEqual(report.BackupsKept, []string{"base"}) {
			t.Fatalf("BackupsKept = %v, want base", report.BackupsKept)
		}
		if len(report.DeleteTargets) != 0 {
			t.Fatalf("DeleteTargets = %v, want none", report.DeleteTargets)
		}
		if _, ok := report.KeepReasons["running"]; ok {
			t.Errorf("running backup should not have a keep reason")
		}
		if _, ok := report.BackupTypes["running"]; ok {
			t.Errorf("running backup should not have a reported type")
		}
	})
}

func TestEvaluateMinKeepHardAbort(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 2
	cfg := config.LifecycleConf{
		Enabled:        true,
		MinKeep:        &minKeep,
		DailyRetention: 1,
	}
	backups := []backup.BackupMeta{
		mockTypedBcp("kept", 0, now, defs.StatusDone, defs.LogicalBackup),
		mockTypedBcp("expired", 10, now, defs.StatusDone, defs.LogicalBackup),
	}

	report := evaluate(cfg, backups, now)
	if !report.Aborted {
		t.Fatal("expected minKeep abort")
	}
	wantReason := "successful restore point count 1 is below minKeep 2"
	if report.AbortReason != wantReason {
		t.Fatalf("AbortReason = %q, want %q", report.AbortReason, wantReason)
	}
	if !reflect.DeepEqual(report.BackupsKept, []string{"kept"}) ||
		!reflect.DeepEqual(report.BackupsPurged, []string{"expired"}) {
		t.Fatalf("unexpected proposed plan: %+v", report)
	}
	if len(report.DeleteTargets) != 0 {
		t.Fatalf("aborted plan has targets %v", report.DeleteTargets)
	}
	if strings.Contains(strings.Join(report.KeepReasons["expired"], ","), "Min Keep") {
		t.Fatal("minKeep must not rescue expired backups")
	}

	text := evaluate(cfg, backups, now).String()
	if !strings.Contains(text, "Status: ABORTED") ||
		!strings.Contains(text, "Proposed backups to PURGE (not executed)") {
		t.Fatalf("aborted report is not explicit:\n%s", text)
	}
}

func TestReportStringDescribesRollingSelection(t *testing.T) {
	report := Report{ConfigUsed: config.LifecycleConf{
		Enabled:          true,
		Strategy:         config.LifecycleStrategyRolling,
		WeeklyRetention:  3,
		MonthlyRetention: 3,
	}}

	text := report.String()
	if !strings.Contains(text, "Weekly: 3 [Newest in rolling bucket]") ||
		!strings.Contains(text, "Monthly: 3 [Newest in rolling bucket]") {
		t.Fatalf("rolling report does not describe bucket selection:\n%s", text)
	}
	if strings.Contains(text, "Auto (Newest in bucket)") {
		t.Fatalf("rolling report contains ambiguous automatic-selection text:\n%s", text)
	}
}

func TestEvaluateMinKeepThresholdAndDisable(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	backups := []backup.BackupMeta{
		mockTypedBcp("kept-1", 0, now, defs.StatusDone, defs.LogicalBackup),
		mockTypedBcp("kept-2", 1, now, defs.StatusDone, defs.LogicalBackup),
		mockTypedBcp("expired", 10, now, defs.StatusDone, defs.LogicalBackup),
	}

	minKeep := 2
	report := evaluate(config.LifecycleConf{
		Enabled:        true,
		DailyRetention: 2,
		MinKeep:        &minKeep,
	}, backups, now)

	if report.Aborted || !reflect.DeepEqual(report.DeleteTargets, []string{"expired"}) {
		t.Fatalf("exact minKeep threshold should remain executable: %+v", report)
	}

	minKeep = 1
	report = evaluate(config.LifecycleConf{
		Enabled:        true,
		DailyRetention: 2,
		MinKeep:        &minKeep,
	}, backups, now)

	if report.Aborted || !reflect.DeepEqual(report.DeleteTargets, []string{"expired"}) {
		t.Fatalf("plan above minKeep should remain executable: %+v", report)
	}

	minKeep = 0
	report = evaluate(config.LifecycleConf{
		Enabled: true,
		MinKeep: &minKeep,
	}, backups, now)

	if report.Aborted || len(report.DeleteTargets) != len(backups) {
		t.Fatalf("minKeep zero should disable the circuit breaker: %+v", report)
	}
}

func TestEvaluateMinKeepGuardRequiresDeletionTargets(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 1

	report := evaluate(config.LifecycleConf{
		Enabled: true,
		MinKeep: &minKeep,
	}, []backup.BackupMeta{
		mockTypedBcp("failed", 10, now, defs.StatusError, defs.LogicalBackup),
	}, now)

	if report.Aborted {
		t.Fatalf("plan without deletion targets should not be aborted: %+v", report)
	}

	report = evaluate(config.LifecycleConf{
		Enabled:     true,
		PurgeFailed: true,
		MinKeep:     &minKeep,
	}, []backup.BackupMeta{
		mockTypedBcp("failed", 10, now, defs.StatusError, defs.LogicalBackup),
	}, now)

	if !report.Aborted {
		t.Fatal("deletion should be blocked while the profile is below minKeep")
	}
	if !reflect.DeepEqual(report.BackupsPurged, []string{"failed"}) {
		t.Fatalf("BackupsPurged = %v, want proposed failed purge", report.BackupsPurged)
	}
	if len(report.DeleteTargets) != 0 {
		t.Fatalf("aborted plan has targets %v", report.DeleteTargets)
	}
}

func TestEvaluateMinKeepCountsOnlyCompleteSuccessfulRestorePoints(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 2
	selective := mockTypedBcp("selective", 0, now, defs.StatusDone, defs.LogicalBackup)
	selective.Namespaces = []string{"db.collection"}
	report := evaluate(config.LifecycleConf{
		Enabled:        true,
		DailyRetention: 1,
		MinKeep:        &minKeep,
	}, []backup.BackupMeta{
		mockTypedBcp("success", 0, now, defs.StatusDone, defs.LogicalBackup),
		selective,
		mockTypedBcp("failed", 10, now, defs.StatusError, defs.LogicalBackup),
		mockTypedBcp("canceled", 10, now, defs.StatusCancelled, defs.LogicalBackup),
		mockTypedBcp("running", 0, now, defs.StatusRunning, defs.LogicalBackup),
		mockTypedBcp("expired", 10, now, defs.StatusDone, defs.LogicalBackup),
	}, now)

	if !report.Aborted {
		t.Fatal("failed, canceled, running, and selective backups must not satisfy minKeep")
	}
	if !reflect.DeepEqual(report.BackupsPurged, []string{"expired"}) {
		t.Fatalf("BackupsPurged = %v, want expired", report.BackupsPurged)
	}
	if len(report.DeleteTargets) != 0 {
		t.Fatalf("aborted plan has targets %v", report.DeleteTargets)
	}
}

func TestEvaluateMinKeepCountsSuccessfulIncrementalRestorePoints(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 2
	report := evaluate(config.LifecycleConf{
		Enabled:        true,
		DailyRetention: 1,
		MinKeep:        &minKeep,
	}, []backup.BackupMeta{
		mockIncrementalBcp("inc-1", "base", 0, now, defs.StatusDone),
		mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
		mockTypedBcp("standalone", 10, now, defs.StatusDone, defs.LogicalBackup),
	}, now)

	if report.Aborted {
		t.Fatalf("two retained incremental restore points should satisfy minKeep: %+v", report)
	}
	if !reflect.DeepEqual(report.BackupsKept, []string{"inc-1", "base"}) {
		t.Fatalf("BackupsKept = %v, want complete incremental chain", report.BackupsKept)
	}
	if !reflect.DeepEqual(report.DeleteTargets, []string{"standalone"}) {
		t.Fatalf("DeleteTargets = %v, want standalone", report.DeleteTargets)
	}
}

func TestEvaluateMinKeepExcludesInvalidIncrementalChains(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 1
	report := evaluate(config.LifecycleConf{
		Enabled: true,
		MinKeep: &minKeep,
	}, []backup.BackupMeta{
		mockIncrementalBcp("orphan", "missing", 1, now, defs.StatusDone),
		mockTypedBcp("valid-standalone", 10, now, defs.StatusDone, defs.LogicalBackup),
	}, now)

	if !report.Aborted {
		t.Fatal("invalid incremental metadata must not satisfy minKeep")
	}
	if !reflect.DeepEqual(report.BackupsKept, []string{"orphan"}) {
		t.Fatalf("BackupsKept = %v, want retained orphan", report.BackupsKept)
	}
	if !reflect.DeepEqual(report.BackupsPurged, []string{"valid-standalone"}) {
		t.Fatalf("BackupsPurged = %v, want proposed valid-standalone purge", report.BackupsPurged)
	}
	if len(report.DeleteTargets) != 0 {
		t.Fatalf("aborted plan has targets %v", report.DeleteTargets)
	}
}

func TestMinKeepCountsPITRProtectedRestorePoint(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 2
	cfg := config.LifecycleConf{
		Enabled:        true,
		DailyRetention: 1,
		MinKeep:        &minKeep,
	}
	backups := []backup.BackupMeta{
		mockTypedBcp("policy-kept", 0, now, defs.StatusDone, defs.LogicalBackup),
		mockTypedBcp("pitr-base", 10, now, defs.StatusDone, defs.LogicalBackup),
		mockTypedBcp("expired", 20, now, defs.StatusDone, defs.LogicalBackup),
	}

	report := evaluateRetentionPolicy(cfg, backups, now)
	report.protectPITRAnchors([]string{"pitr-base"}, backups)
	report.applyMinKeepGuard(backups)

	if report.Aborted {
		t.Fatalf("PITR-protected restore point should satisfy minKeep: %+v", report)
	}
	if !reflect.DeepEqual(report.BackupsKept, []string{"policy-kept", "pitr-base"}) {
		t.Fatalf("BackupsKept = %v, want policy-kept and pitr-base", report.BackupsKept)
	}
	if !reflect.DeepEqual(report.DeleteTargets, []string{"expired"}) {
		t.Fatalf("DeleteTargets = %v, want expired", report.DeleteTargets)
	}
}

func TestMinKeepIsScopedToSelectedProfile(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 2
	cfg := config.LifecycleConf{
		Enabled:        true,
		DailyRetention: 1,
		MinKeep:        &minKeep,
	}
	allBackups := []backup.BackupMeta{
		mockTypedBcp("main-kept", 0, now, defs.StatusDone, defs.LogicalBackup),
		mockTypedBcp("main-expired", 10, now, defs.StatusDone, defs.LogicalBackup),
		mockTypedBcp("profile-kept", 0, now, defs.StatusDone, defs.LogicalBackup),
	}
	allBackups[2].Store.Name = "archive"
	allBackups[2].Store.IsProfile = true
	selectedBackups := filterBackupsByProfile(allBackups, "")

	report := evaluateRetentionPolicy(cfg, selectedBackups, now)
	report.applyMinKeepGuard(selectedBackups)

	if !report.Aborted {
		t.Fatal("a backup in another profile must not satisfy main minKeep")
	}
	if !reflect.DeepEqual(report.BackupsPurged, []string{"main-expired"}) {
		t.Fatalf("BackupsPurged = %v, want main-expired", report.BackupsPurged)
	}
}

func TestEvaluateProtectsInvalidIncrementalChain(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	tests := []struct {
		name        string
		backups     []backup.BackupMeta
		wantKept    []string
		wantReasons map[string][]string
	}{
		{
			name: "missing parent",
			backups: []backup.BackupMeta{
				mockIncrementalBcp("orphan", "missing", 1, now, defs.StatusDone),
			},
			wantKept: []string{"orphan"},
			wantReasons: map[string][]string{
				"orphan": {"Daily", invalidIncrementalChainReason},
			},
		},
		{
			name: "cycle",
			backups: []backup.BackupMeta{
				mockIncrementalBcp("inc-b", "inc-a", 10, now, defs.StatusDone),
				mockIncrementalBcp("inc-a", "inc-b", 11, now, defs.StatusDone),
			},
			wantKept: []string{"inc-b", "inc-a"},
			wantReasons: map[string][]string{
				"inc-b": {invalidIncrementalChainReason},
				"inc-a": {invalidIncrementalChainReason},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := evaluate(config.LifecycleConf{
				Enabled:        true,
				DailyRetention: 3,
				MinKeep:        &minKeep,
			}, tt.backups, now)

			if !reflect.DeepEqual(report.BackupsKept, tt.wantKept) {
				t.Fatalf("BackupsKept = %v, want %v", report.BackupsKept, tt.wantKept)
			}
			for name, want := range tt.wantReasons {
				if !reflect.DeepEqual(report.KeepReasons[name], want) {
					t.Errorf("%s reasons = %v, want %v", name, report.KeepReasons[name], want)
				}
			}
			if len(report.DeleteTargets) != 0 {
				t.Fatalf("DeleteTargets = %v, want none", report.DeleteTargets)
			}
		})
	}
}

func TestEvaluateAllowsFailedAttemptAfterStorageChange(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	minKeep := 0
	backups := []backup.BackupMeta{
		mockIncrementalBcp("failed-attempt", "base", 10, now, defs.StatusError),
		mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
	}
	backups[0].Store.Filesystem.Path = "/other-backups"

	report := evaluate(config.LifecycleConf{
		Enabled:     true,
		PurgeFailed: true,
		MinKeep:     &minKeep,
	}, backups, now)

	wantPurged := []string{"failed-attempt", "base"}
	if !reflect.DeepEqual(report.BackupsPurged, wantPurged) {
		t.Fatalf("BackupsPurged = %v, want %v", report.BackupsPurged, wantPurged)
	}
	if !reflect.DeepEqual(report.DeleteTargets, []string{"base"}) {
		t.Fatalf("DeleteTargets = %v, want base", report.DeleteTargets)
	}
	if len(report.KeepReasons) != 0 {
		t.Fatalf("KeepReasons = %v, want none", report.KeepReasons)
	}
}

func TestEvaluateDisabledIncrementalChain(t *testing.T) {
	now := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)
	report := evaluate(config.LifecycleConf{}, []backup.BackupMeta{
		mockIncrementalBcp("inc-1", "base", 10, now, defs.StatusDone),
		mockIncrementalBcp("base", "", 30, now, defs.StatusDone),
	}, now)

	wantKept := []string{"inc-1", "base"}
	if !reflect.DeepEqual(report.BackupsKept, wantKept) {
		t.Fatalf("BackupsKept = %v, want %v", report.BackupsKept, wantKept)
	}
	if len(report.BackupsPurged) != 0 || len(report.DeleteTargets) != 0 {
		t.Fatalf("disabled lifecycle should not purge backups: %+v", report)
	}
	for _, name := range wantKept {
		if !reflect.DeepEqual(report.KeepReasons[name], []string{"Lifecycle Disabled"}) {
			t.Errorf("%s reasons = %v, want Lifecycle Disabled", name, report.KeepReasons[name])
		}
	}
}

func TestReportHidesDeleteTargetsFromJSON(t *testing.T) {
	b, err := json.Marshal(Report{
		BackupsPurged: []string{"base", "inc-1"},
		DeleteTargets: []string{"base"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "DeleteTargets") || strings.Contains(string(b), "deleteTargets") {
		t.Fatalf("internal deletion targets leaked into JSON: %s", b)
	}
}
