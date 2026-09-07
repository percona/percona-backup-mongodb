package lifecycle

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/util"
)

type Report struct {
	ConfigUsed    config.LifecycleConf `json:"configUsed"`
	Aborted       bool                 `json:"aborted"`
	AbortReason   string               `json:"abortReason,omitempty"`
	BackupsKept   []string             `json:"backupsKept"`
	BackupsPurged []string             `json:"backupsPurged"`
	KeepReasons   map[string][]string  `json:"keepReasons"`
	BackupTypes   map[string]string    `json:"backupTypes"`
	// DeleteTargets contains entry points passed to the checked deletion path. An
	// incremental chain contributes only its base while BackupsPurged lists every member.
	DeleteTargets []string `json:"-"`
}

const (
	incrementalChainReason        = "Incremental Chain Retained"
	invalidIncrementalChainReason = "Invalid Incremental Chain"
	pitrBaseSnapshotReason        = "PITR Base Snapshot"
	afterEvaluationReason         = "Created After Evaluation"
	selectiveBackupReason         = "Selective Backup (Excluded)"
)

type pitrBaseCandidates struct {
	anchors             []string
	previousRestoreTime bson.Timestamp
	restoreTime         bson.Timestamp
}

func (r *Report) addKeepReason(name, reason string) {
	if slices.Contains(r.KeepReasons[name], reason) {
		return
	}
	r.KeepReasons[name] = append(r.KeepReasons[name], reason)
}

func (r *Report) countKeptSuccessfulRestorePoints(backups []backup.BackupMeta) int {
	increments := make(map[string]backup.BackupMeta)
	for _, bcp := range backups {
		if bcp.Type == defs.IncrementalBackup {
			increments[bcp.Name] = bcp
		}
	}

	count := 0
	for _, bcp := range backups {
		reasons := r.KeepReasons[bcp.Name]
		if bcp.Status == defs.StatusDone &&
			!util.IsSelective(bcp.Namespaces) &&
			len(reasons) > 0 &&
			(bcp.Type != defs.IncrementalBackup || isCompleteIncrementalRestorePoint(bcp, increments)) {
			count++
		}
	}
	return count
}

// isCompleteIncrementalRestorePoint verifies that a candidate and all of its
// ancestors form a completed path to the base backup.
func isCompleteIncrementalRestorePoint(
	bcp backup.BackupMeta,
	byName map[string]backup.BackupMeta,
) bool {
	seen := make(map[string]struct{})
	for {
		if bcp.Status != defs.StatusDone {
			return false
		}
		if bcp.SrcBackup == "" {
			return true
		}
		if _, ok := seen[bcp.Name]; ok {
			return false
		}
		seen[bcp.Name] = struct{}{}

		parent, ok := byName[bcp.SrcBackup]
		if !ok {
			return false
		}
		bcp = parent
	}
}

func filterBackupsByProfile(backups []backup.BackupMeta, profile string) []backup.BackupMeta {
	filtered := make([]backup.BackupMeta, 0, len(backups))
	for _, bcp := range backups {
		if profile == "" && bcp.Store.IsProfile {
			continue
		}
		if profile != "" && (!bcp.Store.IsProfile || bcp.Store.Name != profile) {
			continue
		}

		filtered = append(filtered, bcp)
	}

	return filtered
}

// BuildReport loads lifecycle configuration and backup metadata for the main
// storage or a named profile, then builds the report at the provided time.
func BuildReport(
	ctx context.Context,
	conn connect.Client,
	profile string,
	now time.Time,
) (*Report, error) {
	var cfg *config.Config
	var err error
	if profile == "" {
		cfg, err = config.GetConfig(ctx, conn)
		if err != nil {
			return nil, errors.Wrap(err, "get config")
		}
	} else {
		cfg, err = config.GetProfile(ctx, conn, profile)
		if err != nil {
			return nil, errors.Wrap(err, "get profile config")
		}
	}
	allBackups, err := backup.BackupsList(ctx, conn, 0)
	if err != nil {
		return nil, errors.Wrap(err, "fetch backups")
	}

	selectedBackups := filterBackupsByProfile(allBackups, profile)
	report := evaluateRetentionPolicy(*cfg.Lifecycle, selectedBackups, now)

	var pitrEnabled, oplogOnly bool
	if profile == "" {
		pitrEnabled, oplogOnly = cfg.PITR.Enabled, cfg.PITR.OplogOnly
	} else {
		pitrEnabled, oplogOnly, err = config.IsPITREnabled(ctx, conn)
		if err != nil {
			return nil, errors.Wrap(err, "get PITR status")
		}
	}

	if pitrEnabled && !oplogOnly && len(report.DeleteTargets) != 0 {
		err = report.applyPITRDeleteChecks(ctx, conn, selectedBackups)
		if err != nil {
			return nil, errors.Wrap(err, "apply PITR deletion checks")
		}

		if profile == "" && len(report.DeleteTargets) != 0 {
			err = report.applyPITRProtection(ctx, conn, selectedBackups)
			if err != nil {
				return nil, errors.Wrap(err, "apply PITR protection")
			}
		}
	}

	report.applyMinKeepGuard(selectedBackups)
	report.sortDeleteTargets(selectedBackups)
	return report, nil
}

// findIncrementalBase follows source links to the base. Unresolvable links fail
// closed so lifecycle cleanup does not emit a non-base deletion target.
func findIncrementalBase(
	bcp backup.BackupMeta,
	byName map[string]backup.BackupMeta,
) (backup.BackupMeta, bool) {
	seen := make(map[string]struct{})
	for bcp.SrcBackup != "" {
		if _, ok := seen[bcp.Name]; ok {
			return backup.BackupMeta{}, false
		}
		seen[bcp.Name] = struct{}{}

		parent, ok := byName[bcp.SrcBackup]
		if !ok {
			return backup.BackupMeta{}, false
		}
		bcp = parent
	}

	return bcp, bcp.Name != ""
}

// applyIncrementalChainRules expands keep decisions to complete incremental
// chains within the selected storage and protects invalid chain metadata.
func (r *Report) applyIncrementalChainRules(backups []backup.BackupMeta) {
	byName := make(map[string]backup.BackupMeta, len(backups))
	for _, bcp := range backups {
		if bcp.Type == defs.IncrementalBackup {
			byName[bcp.Name] = bcp
		}
	}

	byBase := make(map[string][]backup.BackupMeta)
	for _, bcp := range backups {
		if bcp.Type != defs.IncrementalBackup {
			continue
		}

		base, ok := findIncrementalBase(bcp, byName)
		if !ok {
			if !bcp.Status.IsRunning() {
				r.addKeepReason(bcp.Name, invalidIncrementalChainReason)
			}
			continue
		}
		byBase[base.Name] = append(byBase[base.Name], bcp)
	}

	for _, chain := range byBase {
		retain := false
		for _, member := range chain {
			if len(r.KeepReasons[member.Name]) > 0 || member.Status.IsRunning() {
				retain = true
				break
			}
		}
		if !retain {
			continue
		}

		for _, member := range chain {
			if !member.Status.IsRunning() && len(r.KeepReasons[member.Name]) == 0 {
				r.addKeepReason(member.Name, incrementalChainReason)
			}
		}
	}
}

// isMainPITRBaseSnapshot mirrors automatic PITR base selection. A profile
// backup may still be selected explicitly for restore with --base-snapshot.
func isMainPITRBaseSnapshot(bcp backup.BackupMeta) bool {
	return !bcp.Store.IsProfile &&
		bcp.Status == defs.StatusDone &&
		bcp.Type != defs.ExternalBackup &&
		!util.IsSelective(bcp.Namespaces)
}

// applyPITRDeleteChecks ensures lifecycle never proposes a deletion that the
// checked delete-backup path would reject for PITR safety.
func (r *Report) applyPITRDeleteChecks(
	ctx context.Context,
	conn connect.Client,
	backups []backup.BackupMeta,
) error {
	byName := make(map[string]backup.BackupMeta, len(backups))
	for _, bcp := range backups {
		byName[bcp.Name] = bcp
	}

	protected := make(map[string]struct{})
	for _, name := range r.DeleteTargets {
		bcp, ok := byName[name]
		if !ok {
			return errors.Errorf("PITR deletion target %q not found", name)
		}

		anchor := bcp.Name
		var err error
		if bcp.Type == defs.IncrementalBackup {
			var increments [][]*backup.BackupMeta
			increments, err = backup.FetchAllIncrements(ctx, conn, &bcp)
			if err == nil {
				err = backup.CanDeleteIncrementalChain(ctx, conn, &bcp, increments)
			}
			for _, attempts := range increments {
				for _, increment := range attempts {
					if increment.Status == defs.StatusDone {
						anchor = increment.Name
					}
				}
			}
		} else {
			err = backup.CanDeleteBackup(ctx, conn, &bcp)
		}

		if err == nil {
			continue
		}
		if !errors.Is(err, backup.ErrBaseForPITR) {
			return errors.Wrapf(err, "check whether backup %q can be deleted", name)
		}
		protected[anchor] = struct{}{}
	}

	if len(protected) == 0 {
		return nil
	}

	anchors := make([]string, 0, len(protected))
	for name := range protected {
		anchors = append(anchors, name)
	}
	slices.Sort(anchors)
	r.protectPITRAnchors(anchors, backups)
	return nil
}

// applyPITRProtection retains the newest deletion unit or tied units when they
// are the only bases for the active main-storage PITR timeline.
func (r *Report) applyPITRProtection(
	ctx context.Context,
	conn connect.Client,
	backups []backup.BackupMeta,
) error {
	candidates, err := r.findPITRBaseCandidates(backups)
	if err != nil || len(candidates.anchors) == 0 {
		return err
	}

	timelines, err := oplog.PITRTimelinesBetween(
		ctx,
		conn,
		candidates.previousRestoreTime,
		candidates.restoreTime,
	)
	if err != nil {
		return errors.Wrap(err, "get PITR timelines")
	}
	if !isRequiredPITRBase(candidates.previousRestoreTime, timelines) {
		return nil
	}

	r.protectPITRAnchors(candidates.anchors, backups)
	return nil
}

func (r *Report) protectPITRAnchors(
	anchorNames []string,
	backups []backup.BackupMeta,
) {
	for _, name := range anchorNames {
		r.addKeepReason(name, pitrBaseSnapshotReason)
	}
	// A protected incremental base retains its complete deletion unit.
	r.applyIncrementalChainRules(backups)
	r.buildBackupLists(backups)
}

func (r *Report) findPITRBaseCandidates(backups []backup.BackupMeta) (pitrBaseCandidates, error) {
	purged := make(map[string]struct{}, len(r.BackupsPurged))
	for _, name := range r.BackupsPurged {
		purged[name] = struct{}{}
	}

	var latest []backup.BackupMeta
	var latestWrite bson.Timestamp
	for _, bcp := range backups {
		if _, ok := purged[bcp.Name]; !ok {
			continue
		}
		if !isMainPITRBaseSnapshot(bcp) {
			continue
		}

		switch bcp.LastWriteTS.Compare(latestWrite) {
		case 1:
			latestWrite = bcp.LastWriteTS
			latest = []backup.BackupMeta{bcp}
		case 0:
			latest = append(latest, bcp)
		}
	}
	if len(latest) == 0 {
		return pitrBaseCandidates{}, nil
	}

	var previousSnapshot bson.Timestamp
	for _, bcp := range backups {
		if !isMainPITRBaseSnapshot(bcp) {
			continue
		}
		if _, ok := purged[bcp.Name]; ok {
			continue
		}

		switch bcp.LastWriteTS.Compare(latestWrite) {
		case 1:
			// A newer surviving snapshot can base the active PITR timeline.
			return pitrBaseCandidates{}, nil
		case -1:
			if previousSnapshot.Compare(bcp.LastWriteTS) < 0 {
				previousSnapshot = bcp.LastWriteTS
			}
		}
	}

	byName := make(map[string]backup.BackupMeta, len(backups))
	for _, bcp := range backups {
		if bcp.Type == defs.IncrementalBackup {
			byName[bcp.Name] = bcp
		}
	}

	deleteTargets := make(map[string]struct{}, len(r.DeleteTargets))
	for _, name := range r.DeleteTargets {
		deleteTargets[name] = struct{}{}
	}

	anchorNames := make([]string, 0, len(latest))
	for _, bcp := range latest {
		target := bcp.Name
		if bcp.Type == defs.IncrementalBackup {
			base, ok := findIncrementalBase(bcp, byName)
			if !ok {
				return pitrBaseCandidates{},
					errors.Errorf("resolve PITR base for incremental backup %q", bcp.Name)
			}
			target = base.Name
		}
		if _, ok := deleteTargets[target]; !ok {
			return pitrBaseCandidates{}, errors.Errorf("PITR deletion target %q not found", target)
		}

		anchorNames = append(anchorNames, bcp.Name)
	}
	slices.Sort(anchorNames)

	return pitrBaseCandidates{
		anchors:             anchorNames,
		previousRestoreTime: previousSnapshot,
		restoreTime:         latestWrite,
	}, nil
}

func isRequiredPITRBase(previousRestoreTime bson.Timestamp, timelines []oplog.Timeline) bool {
	return len(timelines) == 1 && previousRestoreTime.T < timelines[0].Start
}

// sortDeleteTargets keeps report targets newest-first so the agent's reverse
// iteration executes deletion units oldest-first by their restore timestamp.
func (r *Report) sortDeleteTargets(backups []backup.BackupMeta) {
	if len(r.DeleteTargets) < 2 {
		return
	}

	targets := make(map[string]struct{}, len(r.DeleteTargets))
	for _, name := range r.DeleteTargets {
		targets[name] = struct{}{}
	}

	byName := make(map[string]backup.BackupMeta, len(backups))
	for _, bcp := range backups {
		if bcp.Type == defs.IncrementalBackup {
			byName[bcp.Name] = bcp
		}
	}

	restoreTS := make(map[string]bson.Timestamp, len(r.DeleteTargets))
	for _, bcp := range backups {
		target := bcp.Name
		if bcp.Type == defs.IncrementalBackup {
			base, ok := findIncrementalBase(bcp, byName)
			if !ok {
				continue
			}
			target = base.Name
		}
		if _, ok := targets[target]; !ok {
			continue
		}
		if bcp.Status == defs.StatusDone && restoreTS[target].Compare(bcp.LastWriteTS) < 0 {
			restoreTS[target] = bcp.LastWriteTS
		}
	}

	slices.SortFunc(r.DeleteTargets, func(a, b string) int {
		if cmp := restoreTS[a].Compare(restoreTS[b]); cmp != 0 {
			return -cmp
		}
		return strings.Compare(b, a)
	})
}

// buildBackupLists rebuilds report projections from the current keep decisions.
// Incremental deletion targets contain only chain bases.
func (r *Report) buildBackupLists(backups []backup.BackupMeta) {
	r.BackupsKept = nil
	r.BackupsPurged = nil
	r.DeleteTargets = nil

	for _, bcp := range backups {
		if bcp.Status.IsRunning() {
			continue
		}

		r.BackupTypes[bcp.Name] = string(bcp.Type)
		if len(r.KeepReasons[bcp.Name]) > 0 {
			r.BackupsKept = append(r.BackupsKept, bcp.Name)
			continue
		}

		r.BackupsPurged = append(r.BackupsPurged, bcp.Name)
		if bcp.Type != defs.IncrementalBackup || bcp.SrcBackup == "" {
			r.DeleteTargets = append(r.DeleteTargets, bcp.Name)
		}
	}
}

// applyMinKeepGuard blocks execution when the retained restore-point count is
// below minKeep. Proposed purge entries remain visible in the report.
func (r *Report) applyMinKeepGuard(backups []backup.BackupMeta) {
	if !r.ConfigUsed.Enabled || len(r.DeleteTargets) == 0 {
		return
	}

	minKeep := r.ConfigUsed.GetMinKeep()
	if minKeep <= 0 {
		return
	}

	kept := r.countKeptSuccessfulRestorePoints(backups)
	if kept >= minKeep {
		return
	}

	r.Aborted = true
	r.AbortReason = fmt.Sprintf(
		"successful restore point count %d is below minKeep %d",
		kept,
		minKeep,
	)
	r.DeleteTargets = nil
}

func (r *Report) String() string {
	cfg := r.ConfigUsed
	strategy := cfg.GetStrategy()

	weeklyStr := "Newest in rolling bucket"
	monthlyStr := "Newest in rolling bucket"

	if strategy == config.LifecycleStrategyCalendar {
		weeklyStr = fmt.Sprintf("Target Day: %d", cfg.WeeklyDay)
		monthlyStr = fmt.Sprintf("Target Date: %d", cfg.MonthlyDay)
	}

	minKeep := cfg.GetMinKeep()

	res := "Lifecycle Report\n"
	res += fmt.Sprintf(
		"Enabled: %v | Strategy: %s | Purge Failed: %v | Min Keep: %d\n",
		cfg.Enabled,
		strings.ToUpper(strategy),
		cfg.PurgeFailed,
		minKeep,
	)
	res += fmt.Sprintf("Daily: %d | Weekly: %d [%s] | Monthly: %d [%s]\n\n",
		cfg.DailyRetention,
		cfg.WeeklyRetention, weeklyStr,
		cfg.MonthlyRetention, monthlyStr)
	if r.Aborted {
		res += fmt.Sprintf("Status: ABORTED | Reason: %s\n\n", r.AbortReason)
	}

	res += fmt.Sprintf("Backups to KEEP (%d):\n", len(r.BackupsKept))
	for _, b := range r.BackupsKept {
		reasons := strings.Join(r.KeepReasons[b], ", ")
		bType := r.BackupTypes[b] // Fetch the type
		res += fmt.Sprintf("  - %s <%s> [%s]\n", b, bType, reasons)
	}

	purgeHeading := "Backups to PURGE"
	if r.Aborted {
		purgeHeading = "Proposed backups to PURGE (not executed)"
	}
	res += fmt.Sprintf("\n%s (%d):\n", purgeHeading, len(r.BackupsPurged))
	for _, b := range r.BackupsPurged {
		bType := r.BackupTypes[b] // Fetch the type
		res += fmt.Sprintf("  - %s <%s>\n", b, bType)
	}
	return res
}

func evaluateRetentionPolicy(
	cfg config.LifecycleConf,
	backups []backup.BackupMeta,
	now time.Time,
) *Report {
	now = now.UTC().Truncate(time.Second)
	report := &Report{
		ConfigUsed:  cfg,
		KeepReasons: make(map[string][]string),
		BackupTypes: make(map[string]string),
	}

	if !cfg.Enabled {
		for _, bcp := range backups {
			if bcp.Status.IsRunning() {
				continue
			}
			report.addKeepReason(bcp.Name, "Lifecycle Disabled")
		}
		report.buildBackupLists(backups)
		return report
	}

	isCalendar := cfg.GetStrategy() == config.LifecycleStrategyCalendar

	dailyCutoff := now.AddDate(0, 0, -cfg.DailyRetention)

	weeklyCandidates := make(map[string][]backup.BackupMeta)
	monthlyCandidates := make(map[string][]backup.BackupMeta)

	for _, bcp := range backups {
		if bcp.Status.IsRunning() {
			continue
		}

		bcpTime := time.Unix(bcp.StartTS, 0).UTC()
		if util.IsSelective(bcp.Namespaces) {
			report.addKeepReason(bcp.Name, selectiveBackupReason)
			continue
		}
		if !bcpTime.Before(now) {
			report.addKeepReason(bcp.Name, afterEvaluationReason)
			continue
		}

		if bcp.Status == defs.StatusError || bcp.Status == defs.StatusCancelled {
			if !cfg.PurgeFailed {
				report.addKeepReason(bcp.Name, "Failed (Protected)")
			} else {
				if cfg.DailyRetention > 0 && !bcpTime.Before(dailyCutoff) {
					report.addKeepReason(bcp.Name, "Failed (Inside Daily Window)")
				}
			}
			continue
		}

		if cfg.DailyRetention > 0 && !bcpTime.Before(dailyCutoff) {
			report.addKeepReason(bcp.Name, "Daily")
		}

		// Weekly Retention Bucket
		if cfg.WeeklyRetention > 0 {
			bucket := rollingBucketIndex(now, bcpTime, 7*24*time.Hour)
			if isCalendar {
				bucket = calendarWeekIndex(now, bcpTime)
			}
			if bucket >= 0 && bucket < cfg.WeeklyRetention {
				weekKey := fmt.Sprintf("week-%d-%s", bucket, bcp.Type)
				weeklyCandidates[weekKey] = append(weeklyCandidates[weekKey], bcp)
			}
		}

		// Monthly Retention Bucket
		if cfg.MonthlyRetention > 0 {
			bucket := rollingBucketIndex(now, bcpTime, 30*24*time.Hour)
			if isCalendar {
				bucket = calendarMonthIndex(now, bcpTime)
			}
			if bucket >= 0 && bucket < cfg.MonthlyRetention {
				monthKey := fmt.Sprintf("month-%d-%s", bucket, bcp.Type)
				monthlyCandidates[monthKey] = append(monthlyCandidates[monthKey], bcp)
			}
		}
	}

	for _, candidates := range weeklyCandidates {
		bestBcp := findBestCandidate(candidates, cfg.WeeklyDay, false, isCalendar)
		if bestBcp != nil {
			report.addKeepReason(bestBcp.Name, "Weekly")
		}
	}

	for _, candidates := range monthlyCandidates {
		bestBcp := findBestCandidate(candidates, cfg.MonthlyDay, true, isCalendar)
		if bestBcp != nil {
			report.addKeepReason(bestBcp.Name, "Monthly")
		}
	}

	// Apply mandatory chain retention before materializing policy projections.
	report.applyIncrementalChainRules(backups)
	report.buildBackupLists(backups)

	return report
}

func rollingBucketIndex(now, backupTime time.Time, width time.Duration) int {
	age := now.Sub(backupTime)
	if age < 0 {
		age = 0
	}
	return int(age / width)
}

func calendarWeekIndex(now, backupTime time.Time) int {
	return int(startOfISOWeek(now).Sub(startOfISOWeek(backupTime)) / (7 * 24 * time.Hour))
}

func startOfISOWeek(t time.Time) time.Time {
	t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	daysSinceMonday := (int(t.Weekday()) + 6) % 7
	return t.AddDate(0, 0, -daysSinceMonday)
}

func calendarMonthIndex(now, backupTime time.Time) int {
	return (now.Year()-backupTime.Year())*12 + int(now.Month()-backupTime.Month())
}

// findBestCandidate selects the optimal backup from a bucket.
func findBestCandidate(
	candidates []backup.BackupMeta,
	targetDayInt int,
	isMonthly bool,
	isCalendar bool,
) *backup.BackupMeta {
	if len(candidates) == 0 {
		return nil
	}

	if !isCalendar {
		// Rolling Option: Pick the newest backup in this bucket
		var best *backup.BackupMeta
		for i := range candidates {
			if betterCandidate(&candidates[i], best) {
				best = &candidates[i]
			}
		}
		return best
	}

	// Calendar Option: Find the backup closest to the targeted day
	var best *backup.BackupMeta
	minDiff := 0
	for i, bcp := range candidates {
		bcpTime := time.Unix(bcp.StartTS, 0).UTC()
		target := calendarTargetDate(bcpTime, targetDayInt, isMonthly)
		diff := dateDistance(bcpTime, target)

		if best == nil || diff < minDiff || diff == minDiff && betterCandidate(&candidates[i], best) {
			minDiff = diff
			best = &candidates[i]
		}
	}

	return best
}

func calendarTargetDate(candidate time.Time, targetDay int, monthly bool) time.Time {
	if !monthly {
		offset := (targetDay - int(time.Monday) + 7) % 7
		return startOfISOWeek(candidate).AddDate(0, 0, offset)
	}

	lastDay := time.Date(candidate.Year(), candidate.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
	if targetDay > lastDay {
		targetDay = lastDay
	}
	return time.Date(candidate.Year(), candidate.Month(), targetDay, 0, 0, 0, 0, time.UTC)
}

func dateDistance(a, b time.Time) int {
	a = time.Date(a.Year(), a.Month(), a.Day(), 0, 0, 0, 0, time.UTC)
	distance := a.Sub(b)
	if distance < 0 {
		distance = -distance
	}
	return int(distance / (24 * time.Hour))
}

func betterCandidate(candidate, current *backup.BackupMeta) bool {
	switch {
	case current == nil:
		return true
	case candidate.LastWriteTS != current.LastWriteTS:
		return candidate.LastWriteTS.After(current.LastWriteTS)
	case candidate.StartTS != current.StartTS:
		return candidate.StartTS > current.StartTS
	default:
		return candidate.Name < current.Name
	}
}
