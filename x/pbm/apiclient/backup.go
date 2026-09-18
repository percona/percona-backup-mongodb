package apiclient

import (
	"context"

	"github.com/percona/percona-backup-mongodb/x/pbm/backup"
	"github.com/percona/percona-backup-mongodb/x/pbm/task"
)

// ListBackups fetches every stored backup metadata document from /backup.
func (c *Client) ListBackups(ctx context.Context) ([]*backup.BackupMeta, error) {
	var metas []*backup.BackupMeta
	if err := c.get(ctx, "/backup", &metas); err != nil {
		return nil, err
	}
	return metas, nil
}

// StartBackup requests a new backup via POST /backup and returns the scheduled
// task. It returns as soon as the backup is delegated to the agents.
func (c *Client) StartBackup(ctx context.Context) (*task.BackupTask, error) {
	t := &task.BackupTask{}
	if err := c.post(ctx, "/backup", t); err != nil {
		return nil, err
	}
	return t, nil
}
