package backup

import (
	"context"
	"log"
	"time"

	"github.com/percona/percona-backup-mongodb/x/pbm/task"
)

// PhysSvc is the physical backup service.
// It orchestrates a physical backup:
// - on the web API servier, it starts the backup cluster-wide,
// - every agent involved in backkup executes backup core logic from Run method.
type PhysSvc struct {
	composer *task.Composer
}

// NewPhysSvc creates the physical backup service.
func NewPhysSvc(composer *task.Composer) *PhysSvc {
	return &PhysSvc{composer: composer}
}

// Start begins a new physical backup and returns as soon as the work is
// delegated.
// Backup itself runs on the agents.
func (s *PhysSvc) Start(ctx context.Context) (*task.BackupTask, error) {
	return s.composer.Backup(ctx, newBackupName())
}

// Run performs this agent's part (core) of the physical backup.
func (s *PhysSvc) Run(ctx context.Context, name string, isLeader bool) error {
	log.Printf("backup: running %s (leader: %t)", name, isLeader)
	time.Sleep(time.Minute)
	return nil
}

// newBackupName renders a backup name for the given time.
func newBackupName() string {
	const BackupNameFormat = "2006-01-02T15:04:05Z"
	return time.Now().UTC().Format(BackupNameFormat)
}
