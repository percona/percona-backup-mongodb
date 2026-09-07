package apiclient

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/percona/percona-backup-mongodb/x/pbm/config"
)

// GetConfig fetches the config document under name from /config/{name}. It
// returns ErrNotFound when no such config exists.
func (c *Client) GetConfig(ctx context.Context, name string) (*config.Config, error) {
	cfg := &config.Config{}
	if err := c.get(ctx, "/config/"+name, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// ListConfigs fetches every stored config document from /config.
func (c *Client) ListConfigs(ctx context.Context) ([]*config.Config, error) {
	var cfgs []*config.Config
	if err := c.get(ctx, "/config", &cfgs); err != nil {
		return nil, err
	}
	return cfgs, nil
}

// SetConfig upserts cfg. The name defaults to the default config name when cfg.Name is empty.
func (c *Client) SetConfig(ctx context.Context, cfg *config.Config) error {
	body, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	name := cfg.Name
	if name == "" {
		name = config.DefaultConfigName
	}
	return c.put(ctx, "/config/"+name, body)
}

// ResyncConfig forces a backup-list resync for the named config via
// POST /config/{name}/resync. It returns ErrNotFound when no such config exists.
func (c *Client) ResyncConfig(ctx context.Context, name string) error {
	return c.post(ctx, "/config/"+name+"/resync")
}
