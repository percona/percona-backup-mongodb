package lifecycle

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
)

type Report struct {
	DryRun        bool                 `json:"dryRun"`
	ConfigUsed    config.LifecycleConf `json:"configUsed"`
	BackupsKept   []string             `json:"backupsKept"`
	BackupsPurged []string             `json:"backupsPurged"`
	KeepReasons   map[string][]string  `json:"keepReasons"`
	BackupTypes   map[string]string    `json:"backupTypes"`
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
		reasons := strings.Join(r.KeepReasons[b], ", ")
		bType := r.BackupTypes[b] // Fetch the type
		res += fmt.Sprintf("  - %s <%s> [%s]\n", b, bType, reasons)
	}

	res += fmt.Sprintf("\nBackups to PURGE (%d):\n", len(r.BackupsPurged))
	for _, b := range r.BackupsPurged {
		bType := r.BackupTypes[b] // Fetch the type
		res += fmt.Sprintf("  - %s <%s>\n", b, bType)
	}
	return res
}

// Replace the Evaluate function (~ line 50) with this:
func Evaluate(cfg config.LifecycleConf, backups []backup.BackupMeta, dryRun bool, now time.Time) *Report {
	report := &Report{
		DryRun:      dryRun,
		ConfigUsed:  cfg,
		KeepReasons: make(map[string][]string),
		BackupTypes: make(map[string]string),
	}

	// BUG FIX: If Disabled, go to sleep. Keep everything.
	if !cfg.Enabled {
		for _, bcp := range backups {
			if bcp.Status.IsRunning() {
				continue
			}
			report.BackupTypes[bcp.Name] = string(bcp.Type)
			report.BackupsKept = append(report.BackupsKept, bcp.Name)
			report.KeepReasons[bcp.Name] = []string{"Lifecycle Disabled"}
		}
		return report
	}

	isCalendar := strings.ToLower(cfg.Strategy) == "calendar"
	keepMap := make(map[string][]string)

	addReason := func(name, reason string) {
		for _, r := range keepMap[name] {
			if r == reason {
				return
			}
		}
		keepMap[name] = append(keepMap[name], reason)
	}

	dailyCutoff := now.AddDate(0, 0, -cfg.DailyRetention)
	weeklyCutoff := now.AddDate(0, 0, -(cfg.WeeklyRetention * 7))
	monthlyCutoff := now.AddDate(0, -cfg.MonthlyRetention, 0)

	weeklyCandidates := make(map[string][]backup.BackupMeta)
	monthlyCandidates := make(map[string][]backup.BackupMeta)

	// 1. Bucketing phase
	for _, bcp := range backups {
		if bcp.Status.IsRunning() {
			addReason(bcp.Name, "In-Progress")
			continue
		}

		bcpTime := time.Unix(bcp.StartTS, 0).UTC()
		ageInDays := int(now.Sub(bcpTime).Hours() / 24)

		if bcp.Status == defs.StatusError || bcp.Status == defs.StatusCancelled {
			if !cfg.PurgeFailed {
				addReason(bcp.Name, "Failed (Protected)")
			} else {
				if cfg.DailyRetention > 0 && !bcpTime.Before(dailyCutoff) {
					addReason(bcp.Name, "Failed (Inside Daily Window)")
				}
			}
			continue
		}

		if cfg.DailyRetention > 0 && !bcpTime.Before(dailyCutoff) {
			addReason(bcp.Name, "Daily")
			continue
		}

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

	for _, candidates := range weeklyCandidates {
		bestBcp := findBestCandidate(candidates, cfg.WeeklyDay, false, isCalendar)
		if bestBcp != nil {
			addReason(bestBcp.Name, "Weekly")
		}
	}

	for _, candidates := range monthlyCandidates {
		bestBcp := findBestCandidate(candidates, cfg.MonthlyDay, true, isCalendar)
		if bestBcp != nil {
			addReason(bestBcp.Name, "Monthly")
		}
	}

	// 3. Finalize Lists
	for _, bcp := range backups {
		if bcp.Status.IsRunning() {
			continue
		}

		report.BackupTypes[bcp.Name] = string(bcp.Type)

		if len(keepMap[bcp.Name]) > 0 {
			report.BackupsKept = append(report.BackupsKept, bcp.Name)
			report.KeepReasons[bcp.Name] = keepMap[bcp.Name]
		} else {
			report.BackupsPurged = append(report.BackupsPurged, bcp.Name)
		}
	}

	// BUG FIX: 4. Enforce Min Keep (Rescue backups from PURGE)
	minKeep := 1
	if cfg.MinKeep != nil {
		minKeep = *cfg.MinKeep
	}

	if minKeep > 0 && len(report.BackupsKept) < minKeep {
		var rescue []backup.BackupMeta
		for _, bcp := range backups {
			if bcp.Status == defs.StatusDone && len(keepMap[bcp.Name]) == 0 {
				rescue = append(rescue, bcp)
			}
		}

		// Sort newest first to rescue the most recent ones
		slices.SortFunc(rescue, func(a, b backup.BackupMeta) int {
			if a.StartTS > b.StartTS {
				return -1
			}
			if a.StartTS < b.StartTS {
				return 1
			}
			return 0
		})

		for _, bcp := range rescue {
			if len(report.BackupsKept) >= minKeep {
				break
			}
			report.BackupsKept = append(report.BackupsKept, bcp.Name)
			report.KeepReasons[bcp.Name] = []string{"Min Keep"}

			// Remove from Purged list safely
			report.BackupsPurged = slices.DeleteFunc(report.BackupsPurged, func(name string) bool {
				return name == bcp.Name
			})
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
