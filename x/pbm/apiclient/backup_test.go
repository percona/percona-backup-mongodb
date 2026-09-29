package apiclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/percona/percona-backup-mongodb/x/pbm/backup"
	"github.com/percona/percona-backup-mongodb/x/pbm/compress"
	"github.com/percona/percona-backup-mongodb/x/pbm/defs"
	"github.com/percona/percona-backup-mongodb/x/pbm/task"
)

func TestStartBackup(t *testing.T) {
	t.Run("sends the options", func(t *testing.T) {
		level := 6
		want := backup.Options{
			Type:             defs.PhysicalBackup,
			Compression:      compress.CompressionTypePGZIP,
			CompressionLevel: &level,
			NumParallelFiles: 4,
			Profile:          "main",
		}

		var got backup.Options
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/backup" {
				t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			}
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Errorf("decode options: %v", err)
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(task.BackupTask{Name: "2026-04-14T14:07:00Z"})
		}))
		defer srv.Close()

		bcpTask, err := New([]string{srv.URL}).StartBackup(context.Background(), want)
		if err != nil {
			t.Fatalf("StartBackup: %v", err)
		}
		if bcpTask.Name != "2026-04-14T14:07:00Z" {
			t.Errorf("Name = %q, want %q", bcpTask.Name, "2026-04-14T14:07:00Z")
		}

		if got.CompressionLevel == nil || *got.CompressionLevel != level {
			t.Fatalf("CompressionLevel = %v, want %d", got.CompressionLevel, level)
		}
		got.CompressionLevel = want.CompressionLevel
		if got != want {
			t.Errorf("options = %+v, want %+v", got, want)
		}
	})

	t.Run("only physical is supported", func(t *testing.T) {
		reason := `backup type "logical": only "physical" is supported`
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "invalid backup options: "+reason, http.StatusBadRequest)
		}))
		defer srv.Close()

		_, err := New([]string{srv.URL}).StartBackup(context.Background(),
			backup.Options{Type: defs.LogicalBackup})
		if !errors.Is(err, ErrBadRequest) {
			t.Fatalf("StartBackup: got %v, want ErrBadRequest", err)
		}
		// the API's reason is what the user gets to read.
		if !strings.Contains(err.Error(), reason) {
			t.Errorf("error %q carries no reason", err)
		}
	})

	t.Run("invalid options", func(t *testing.T) {
		reason := "number of parallel files -1: must be positive"
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "invalid backup options: "+reason, http.StatusBadRequest)
		}))
		defer srv.Close()

		_, err := New([]string{srv.URL}).StartBackup(context.Background(),
			backup.Options{Type: defs.PhysicalBackup, NumParallelFiles: -1})
		if !errors.Is(err, ErrBadRequest) {
			t.Fatalf("StartBackup: got %v, want ErrBadRequest", err)
		}
		// the API's reason is what the user gets to read.
		if !strings.Contains(err.Error(), reason) {
			t.Errorf("error %q carries no reason", err)
		}
	})
}
