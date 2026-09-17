// Package task holds PBM's task state in etcd: the record of a running
// cluster-wide task and the per-agent inboxes through which the composer hands
// work out to the agents.
//
// The key layout is:
//
//	/pbm/tasks/backups/<backup-name>          the cluster-wide backup task
//	/pbm/tasks/inbox/<agent-id>/<backup-name> one agent's inbox item
//
// All keys of a single backup share one lease, which the participating agents
// keep alive while they work. When every agent is gone the lease expires and
// the task leaves no stale trace behind.
package task

const (
	backupsPrefix = "/pbm/tasks/backups/"
	inboxPrefix   = "/pbm/tasks/inbox/"
)

// BackupTask is the cluster-wide record of a running backup. It is the trace
// the composer leaves in the control state so the rest of the cluster can
// tell which backup runs and who takes part in it.
type BackupTask struct {
	Name string `json:"name"`
	// Agents are the agents taking part in the backup, one per replica set.
	Agents []string `json:"agents"`
	// Leader is the agent coordinating the backup within the cluster
	Leader  string `json:"leader"`
	StartTS int64  `json:"start_ts"`
}

type TaskType string

const (
	TaskBackup  TaskType = "backup"
	TaskRestore TaskType = "restore"
)

// InboxItem is one agent's copy of the work.
type InboxItem struct {
	Type     TaskType `json:"type"`
	Task     string   `json:"task"`
	IsLeader bool     `json:"leader"`
}

// backupKey resolves the backup name to the task key.
func backupKey(name string) string {
	return backupsPrefix + name
}

// inboxKey resolves an agent's inbox item for the named backup.
func inboxKey(agentID, backupName string) string {
	return inboxPrefix + agentID + "/" + backupName
}

// agentInboxPrefix is the prefix an agent watches for its own inbox items.
func agentInboxPrefix(agentID string) string {
	return inboxPrefix + agentID + "/"
}
