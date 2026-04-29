package lifecycle

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
)

// mockBcp is a helper to generate fake backups.
// daysAgo subtracts from the baseTime (our frozen mockNow).
func mockBcp(name string, daysAgo int, baseTime time.Time, status defs.Status) backup.BackupMeta {
	bcpTime := baseTime.AddDate(0, 0, -daysAgo)
	return backup.BackupMeta{
		Name:    name,
		StartTS: bcpTime.Unix(),
		Status:  status,
	}
}

// mockTypedBcp is a helper to generate fake backups with a specific Type.
func mockTypedBcp(name string, daysAgo int, baseTime time.Time, status defs.Status, bcpType defs.BackupType) backup.BackupMeta {
	bcp := mockBcp(name, daysAgo, baseTime, status)
	bcp.Type = bcpType
	return bcp
}

func TestEvaluate(t *testing.T) {
	// Define a few specific dates we want to test against
	standardDate := time.Date(2026, time.March, 26, 12, 0, 0, 0, time.UTC)     // A normal Thursday
	leapYearDate := time.Date(2024, time.February, 29, 12, 0, 0, 0, time.UTC)  // Leap Day
	endOfYearDate := time.Date(2025, time.December, 31, 12, 0, 0, 0, time.UTC) // Dec 31st

	tests := []struct {
		name           string
		cfg            config.LifecycleConf
		backups        []backup.BackupMeta
		dryRun         bool
		mockNow        time.Time
		expectedKept   []string
		expectedPurged []string
	}{
		// --- STANDARD DATE SCENARIOS ---
		{
			name: "Feature Disabled (Dry run or not, it sleeps)",
			cfg: config.LifecycleConf{
				Enabled:        false,
				DailyRetention: 1,
			},
			mockNow: standardDate,
			backups: []backup.BackupMeta{
				mockBcp("bcp-today", 0, standardDate, defs.StatusDone),
				mockBcp("bcp-old", 10, standardDate, defs.StatusDone),
			},
			dryRun:         true,
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
			dryRun:         false,
			expectedKept:   []string{"bcp-today", "bcp-7-days", "bcp-9-days"},
			expectedPurged: []string{"bcp-12-days"},
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
			dryRun:         false,
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
			dryRun:         false,
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
			dryRun:         false,
			expectedKept:   []string{"bcp-dec-2"},
			expectedPurged: []string{"bcp-nov-29"},
		},

		// --- STATE HANDLING SCENARIOS ---
		{
			name: "In-Progress Backups are ALWAYS protected (and MinKeep rescues the last safe base)",
			cfg: config.LifecycleConf{
				Enabled:        true,
				DailyRetention: 1,
			},
			mockNow: standardDate,
			backups: []backup.BackupMeta{
				mockBcp("bcp-running", 0, standardDate, defs.StatusRunning), // In-progress today
				mockBcp("bcp-done-old", 50, standardDate, defs.StatusDone),  // 50 days old, normally purged
			},
			dryRun: false,
			// bcp-running is implicitly protected (hidden from purge).
			// bcp-done-old is expired, but rescued because MinKeep defaults to 1 and the running backup doesn't count yet!
			expectedKept:   []string{"bcp-done-old"},
			expectedPurged: []string{},
		},
		{
			name: "Failed Backups - PurgeFailed is TRUE",
			cfg: config.LifecycleConf{
				Enabled:        true,
				PurgeFailed:    true,
				DailyRetention: 7, // Protect for 7 days
			},
			mockNow: standardDate,
			backups: []backup.BackupMeta{
				mockBcp("bcp-error-recent", 3, standardDate, defs.StatusError), // Kept
				mockBcp("bcp-error-old", 10, standardDate, defs.StatusError),   // Purged
			},
			dryRun:         false,
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
			dryRun:         false,
			expectedKept:   []string{"phys-newer", "logical-only"},
			expectedPurged: []string{"phys-older"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := Evaluate(tt.cfg, tt.backups, tt.dryRun, tt.mockNow)

			if report.DryRun != tt.dryRun {
				t.Errorf("Report.DryRun = %v, want %v", report.DryRun, tt.dryRun)
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
