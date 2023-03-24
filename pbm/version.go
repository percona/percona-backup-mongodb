package pbm

// BreakingChangesMap, map of versions introduced breaking changes to respective
// backup types.
// !!! Versions should be sorted in the ascending order.
var BreakingChangesMap = map[BackupType][]string{
	LogicalBackup:     {"1.5.0"},
	IncrementalBackup: {"2.1.0"},
	PhysicalBackup:    {},
}
