package apiclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/config/fs"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage"
)

func testConfig(name, path string) *config.Config {
	return &config.Config{
		Name: name,
		Storage: config.StorageConf{
			Type:       storage.Filesystem,
			Filesystem: &fs.Config{Path: path},
		},
	}
}

func TestGetConfig(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/config/main" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(testConfig("main", "/data"))
	}))
	defer srv.Close()

	got, err := New([]string{srv.URL}).GetConfig(context.Background(), "main")
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if got.Name != "main" || got.Storage.Filesystem == nil || got.Storage.Filesystem.Path != "/data" {
		t.Fatalf("unexpected config: %+v", got)
	}
}

func TestGetConfigNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "config not found", http.StatusNotFound)
	}))
	defer srv.Close()

	_, err := New([]string{srv.URL}).GetConfig(context.Background(), "ghost")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetConfig: got %v, want ErrNotFound", err)
	}
}

func TestListConfigs(t *testing.T) {
	want := []*config.Config{testConfig("a", "/a"), testConfig("b", "/b")}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/config" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	got, err := New([]string{srv.URL}).ListConfigs(context.Background())
	if err != nil {
		t.Fatalf("ListConfigs: %v", err)
	}
	if len(got) != 2 || got[0].Name != "a" || got[1].Name != "b" {
		t.Fatalf("unexpected configs: %+v", got)
	}
}

func TestSetConfig(t *testing.T) {
	var gotMethod, gotPath string
	var gotBody config.Config
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &gotBody)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	err := New([]string{srv.URL}).SetConfig(context.Background(), testConfig("main", "/data"))
	if err != nil {
		t.Fatalf("SetConfig: %v", err)
	}
	if gotMethod != http.MethodPut {
		t.Errorf("method = %s, want PUT", gotMethod)
	}
	if gotPath != "/config/main" {
		t.Errorf("path = %s, want /config/main", gotPath)
	}
	if gotBody.Storage.Filesystem == nil || gotBody.Storage.Filesystem.Path != "/data" {
		t.Errorf("unexpected body: %+v", gotBody)
	}
}

func TestResyncConfig(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	if err := New([]string{srv.URL}).ResyncConfig(context.Background(), "main"); err != nil {
		t.Fatalf("ResyncConfig: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %s, want POST", gotMethod)
	}
	if gotPath != "/config/main/resync" {
		t.Errorf("path = %s, want /config/main/resync", gotPath)
	}
}

func TestResyncConfigNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "config not found", http.StatusNotFound)
	}))
	defer srv.Close()

	err := New([]string{srv.URL}).ResyncConfig(context.Background(), "xyz")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("ResyncConfig: got %v, want ErrNotFound", err)
	}
}

func TestSetConfigDefaultsName(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	// Empty name must map to the default config name in the path.
	err := New([]string{srv.URL}).SetConfig(context.Background(), testConfig("", "/data"))
	if err != nil {
		t.Fatalf("SetConfig: %v", err)
	}
	if gotPath != "/config/"+config.DefaultConfigName {
		t.Errorf("path = %s, want /config/%s", gotPath, config.DefaultConfigName)
	}
}
