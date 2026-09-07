package apiclient

import (
	"context"

	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

// Status fetches every cluster member from the leader's /status endpoint.
func (c *Client) Status(ctx context.Context) ([]status.AgentInfo, error) {
	var members []status.AgentInfo
	if err := c.get(ctx, "/status", &members); err != nil {
		return nil, err
	}
	return members, nil
}
