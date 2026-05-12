package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/log"
)

const (
	EventStart = "start"
	EventDone  = "done"
	EventError = "error"

	sendTimeout = 30 * time.Second
)

type Notification struct {
	Type       string `json:"type"`
	Event      string `json:"event"`
	Name       string `json:"name"`
	Error      string `json:"error,omitempty"`
	BackupType string `json:"backup_type,omitempty"`
}

type Notifier struct {
	ctx context.Context
	cfg *config.WebhooksConf
	l   log.LogEvent
}

func NewNotifier(ctx context.Context, cfg *config.WebhooksConf, l log.LogEvent) *Notifier {
	return &Notifier{ctx: ctx, cfg: cfg, l: l}
}

func (w *Notifier) SendBackup(event, name string, err error) {
	if w == nil || w.cfg == nil || w.cfg.URL == "" {
		return
	}
	if !w.cfg.Backups.Events.Has(event) {
		return
	}

	n := Notification{
		Type:  "backup",
		Event: event,
		Name:  name,
	}
	if err != nil {
		n.Error = err.Error()
	}

	w.send(n)
}

func (w *Notifier) SendRestore(event, name string, err error) {
	if w == nil || w.cfg == nil || w.cfg.URL == "" {
		return
	}
	if !w.cfg.Restores.Events.Has(event) {
		return
	}

	n := Notification{
		Type:  "restore",
		Event: event,
		Name:  name,
	}
	if err != nil {
		n.Error = err.Error()
	}

	w.send(n)
}

type slackPayload struct {
	Text string `json:"text"`
}

func (n Notification) formatText() string {
	msg := fmt.Sprintf("PBM %s *%s*: %s", n.Type, n.Event, n.Name)
	if n.Error != "" {
		msg += fmt.Sprintf("\nError: %s", n.Error)
	}
	return msg
}

func (w *Notifier) send(n Notification) {
	body, err := json.Marshal(slackPayload{Text: n.formatText()})
	if err != nil {
		w.l.Warning("webhook notification: marshal: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(w.ctx, sendTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.cfg.URL, bytes.NewReader(body))
	if err != nil {
		w.l.Warning("webhook notification: create request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		w.l.Warning("webhook notification: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		w.l.Warning("webhook notification: unexpected status %d", resp.StatusCode)
	}
}
