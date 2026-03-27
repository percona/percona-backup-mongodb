package lifecycle

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
)

type Report struct {
	DryRun        bool
	ConfigUsed    config.LifecycleConf
	BackupsKept   []string
	BackupsPurged []string
}

func (r *Report) String() string {
	strategy := strings.ToLower(r.ConfigUsed.Strategy)
	if strategy == "" {
		strategy = "rolling" // Default
	}

	weeklyStr := "Auto (Newest in bucket)"
	monthlyStr := "Auto (Newest in bucket)"

	if strategy == "calendar" {
		weeklyStr = fmt.Sprintf("Target Day: %d", r.ConfigUsed.WeeklyDay)
		monthlyStr = fmt.Sprintf("Target Date: %d", r.ConfigUsed.MonthlyDay)
	}

	minKeep := 1
	if r.ConfigUsed.MinKeep != nil {
		minKeep = *r.ConfigUsed.MinKeep
	}

	res := fmt.Sprintf("Lifecycle Report (Dry Run: %v)\n", r.DryRun)
	res += fmt.Sprintf("Enabled: %v | Strategy: %s | Purge Failed: %v | Min Keep: %d\n", r.ConfigUsed.Enabled, strings.ToUpper(strategy), r.ConfigUsed.PurgeFailed, minKeep)
	res += fmt.Sprintf("Daily: %d | Weekly: %d [%s] | Monthly: %d [%s]\n\n",
		r.ConfigUsed.DailyRetention,
		r.ConfigUsed.WeeklyRetention, weeklyStr,
		r.ConfigUsed.MonthlyRetention, monthlyStr)

	res += fmt.Sprintf("Backups to KEEP (%d):\n", len(r.BackupsKept))
	for _, b := range r.BackupsKept {
		res += fmt.Sprintf("  - %s\n", b)
	}

	res += fmt.Sprintf("\nBackups to PURGE (%d):\n", len(r.BackupsPurged))
	for _, b := range r.BackupsPurged {
		res += fmt.Sprintf("  - %s\n", b)
	}
	return res
}

// Evaluate determines which backups to keep and which to purge based on the config.
func Evaluate(cfg config.LifecycleConf, backups []backup.BackupMeta, dryRun bool, now time.Time) *Report {
	report := &Report{
		DryRun:     dryRun,
		ConfigUsed: cfg,
	}

	// Exit early if disabled and not a dry run.
	if !cfg.Enabled && !dryRun {
		return report
	}

	isCalendar := strings.ToLower(cfg.Strategy) == "calendar"

	keepMap := make(map[string]bool)

	dailyCutoff := now.AddDate(0, 0, -cfg.DailyRetention)
	weeklyCutoff := now.AddDate(0, 0, -(cfg.WeeklyRetention * 7))
	monthlyCutoff := now.AddDate(0, -cfg.MonthlyRetention, 0)

	weeklyCandidates := make(map[string][]backup.BackupMeta)
	monthlyCandidates := make(map[string][]backup.BackupMeta)

	// 1. Bucketing phase
	for _, bcp := range backups {
		// ALWAYS protect in-progress backups. They are never purged.
		if bcp.Status.IsRunning() {
			keepMap[bcp.Name] = true
			continue
		}

		bcpTime := time.Unix(bcp.StartTS, 0).UTC()
		ageInDays := int(now.Sub(bcpTime).Hours() / 24)

		// Handle Failed/Cancelled backups
		if bcp.Status == defs.StatusError || bcp.Status == defs.StatusCancelled {
			if !cfg.PurgeFailed {
				keepMap[bcp.Name] = true // Protect failed backups if PurgeFailed is false
			} else {
				// If PurgeFailed is true, only keep them if they fall into the Daily window
				if cfg.DailyRetention > 0 && bcpTime.After(dailyCutoff) {
					keepMap[bcp.Name] = true
				}
			}
			continue // Do not evaluate failed backups for Weekly/Monthly
		}

		// Daily Retention (Completed Backups)
		if cfg.DailyRetention > 0 && !bcpTime.Before(dailyCutoff) {
			keepMap[bcp.Name] = true
			continue
		}

		// Weekly Retention Bucket
		if cfg.WeeklyRetention > 0 && !bcpTime.Before(weeklyCutoff) {
			if isCalendar {
				year, week := bcpTime.ISOWeek()
				weekKey := fmt.Sprintf("calendar-week-%d-W%02d", year, week)
				weeklyCandidates[weekKey] = append(weeklyCandidates[weekKey], bcp)
			} else {
				weekBucket := ageInDays / 7
				weekKey := fmt.Sprintf("rolling-week-%d", weekBucket)
				weeklyCandidates[weekKey] = append(weeklyCandidates[weekKey], bcp)
			}
		}

		// Monthly Retention Bucket
		if cfg.MonthlyRetention > 0 && !bcpTime.Before(monthlyCutoff) {
			if isCalendar {
				monthKey := bcpTime.Format("2006-01")
				monthlyCandidates[monthKey] = append(monthlyCandidates[monthKey], bcp)
			} else {
				monthBucket := ageInDays / 30
				monthKey := fmt.Sprintf("rolling-month-%d", monthBucket)
				monthlyCandidates[monthKey] = append(monthlyCandidates[monthKey], bcp)
			}
		}
	}

	// 2. Select best candidates for Weekly & Monthly
	for _, candidates := range weeklyCandidates {
		bestBcp := findBestCandidate(candidates, cfg.WeeklyDay, false, isCalendar)
		if bestBcp != nil {
			keepMap[bestBcp.Name] = true
		}
	}

	for _, candidates := range monthlyCandidates {
		bestBcp := findBestCandidate(candidates, cfg.MonthlyDay, true, isCalendar)
		if bestBcp != nil {
			keepMap[bestBcp.Name] = true
		}
	}

	// 3. Finalize Lists
	for _, bcp := range backups {
		if bcp.Status.IsRunning() {
			continue // Hide in-progress backups from the Keep/Purge report
		}

		if keepMap[bcp.Name] {
			report.BackupsKept = append(report.BackupsKept, bcp.Name)
		} else {
			report.BackupsPurged = append(report.BackupsPurged, bcp.Name)
		}
	}

	return report
}

// findBestCandidate selects the optimal backup from a bucket.
func findBestCandidate(candidates []backup.BackupMeta, targetDayInt int, isMonthly bool, isCalendar bool) *backup.BackupMeta {
	if len(candidates) == 0 {
		return nil
	}

	var best *backup.BackupMeta

	if !isCalendar {
		// Rolling Option: Pick the newest backup in this bucket
		for i := range candidates {
			if best == nil || candidates[i].StartTS > best.StartTS {
				best = &candidates[i]
			}
		}
		return best
	}

	// Calendar Option: Find the backup closest to the targeted day
	minDiff := 31 // Max possible difference
	for i, bcp := range candidates {
		bcpTime := time.Unix(bcp.StartTS, 0).UTC()
		diff := 0

		if isMonthly {
			diff = int(math.Abs(float64(bcpTime.Day() - targetDayInt)))
		} else {
			diff = int(math.Abs(float64(bcpTime.Weekday() - time.Weekday(targetDayInt))))
		}

		if diff < minDiff {
			minDiff = diff
			best = &candidates[i]
		}
	}

	return best
}
