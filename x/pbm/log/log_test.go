package log

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestNewInvalid(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"unknown level", Config{Level: "verbose"}},
		{"unsupported zap level", Config{Level: "dpanic"}},
		{"unknown format", Config{Format: "text"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.cfg); err == nil {
				t.Errorf("New(%+v) = nil error, want error", tt.cfg)
			}
		})
	}
}

func TestNewWritesToFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pbmx.log")

	l, err := New(Config{Outputs: []string{path}, Level: "warn", Format: JSONFormat})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	l.Info("dropped")
	l.Warn("kept")
	_ = l.Sync()

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d log lines, want 1:\n%s", len(lines), b)
	}

	var entry struct {
		Level string `json:"level"`
		Msg   string `json:"msg"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("decode %q: %v", lines[0], err)
	}
	if entry.Level != "warn" || entry.Msg != "kept" {
		t.Errorf("got entry %+v, want level=warn msg=kept", entry)
	}
}

func TestOutputPaths(t *testing.T) {
	tests := []struct {
		in   []string
		want []string
	}{
		{nil, []string{"stderr"}},
		{[]string{"stderr", "stdout", "stderr"}, []string{"stderr", "stdout"}},
		{[]string{"/var/log/pbmx.log"}, []string{"/var/log/pbmx.log"}},
	}

	for _, tt := range tests {
		if got := outputPaths(tt.in); !slices.Equal(got, tt.want) {
			t.Errorf("outputPaths(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
