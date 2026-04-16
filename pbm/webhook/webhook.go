package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/log"
)

const sendTimeout = 30 * time.Second

type Notification struct {
	Type       string `json:"type"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
	BackupType string `json:"backup_type,omitempty"`
}

func Send(ctx context.Context, cfg *config.WebhooksConf, n Notification, l log.LogEvent) {
	if cfg == nil || cfg.URL == "" {
		return
	}
	if n.Type == "backup" && !cfg.Backups.Enabled {
		return
	}
	if n.Type == "restore" && !cfg.Restores.Enabled {
		return
	}

	body, err := json.Marshal(n)
	if err != nil {
		l.Warning("webhook notification: marshal: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(body))
	if err != nil {
		l.Warning("webhook notification: create request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		l.Warning("webhook notification: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		l.Warning("webhook notification: unexpected status %d", resp.StatusCode)
	}
}
