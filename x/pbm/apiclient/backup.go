package apiclient

import (
	"context"

	"github.com/percona/percona-backup-mongodb/x/pbm/backup"
)

// ListBackups fetches every stored backup metadata document from /backup.
func (c *Client) ListBackups(ctx context.Context) ([]*backup.BackupMeta, error) {
	var metas []*backup.BackupMeta
	if err := c.get(ctx, "/backup", &metas); err != nil {
		return nil, err
	}
	return metas, nil
}
