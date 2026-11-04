package main

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/percona/percona-backup-mongodb/x/pbm/apiclient"
	"github.com/percona/percona-backup-mongodb/x/pbm/backup"
	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/config/fs"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage"
)

func TestParseSections(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		tests := []struct {
			name  string
			debug bool
			want  []string
		}{
			{name: "without debug", want: []string{"backups", "cluster", "pitr", "running"}},
			{
				name:  "with debug",
				debug: true,
				want:  []string{"backups", "cluster", "debug", "pitr", "running"},
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := parseSections(nil, tc.debug)
				if err != nil {
					t.Fatalf("parseSections: %v", err)
				}
				if keys := sortedKeys(got); !slices.Equal(keys, tc.want) {
					t.Errorf("sections = %v, want %v", keys, tc.want)
				}
			})
		}
	})

	t.Run("explicit sections", func(t *testing.T) {
		tests := []struct {
			name  string
			names []string
			debug bool
			want  []string
		}{
			{name: "single", names: []string{"pitr"}, want: []string{"pitr"}},
			{
				name:  "several",
				names: []string{"running", "cluster"},
				want:  []string{"cluster", "running"},
			},
			{name: "duplicates", names: []string{"backups", "backups"}, want: []string{"backups"}},
			{
				name:  "debug flag adds debug",
				names: []string{"cluster"},
				debug: true,
				want:  []string{"cluster", "debug"},
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				got, err := parseSections(tc.names, tc.debug)
				if err != nil {
					t.Fatalf("parseSections: %v", err)
				}
				if keys := sortedKeys(got); !slices.Equal(keys, tc.want) {
					t.Errorf("sections = %v, want %v", keys, tc.want)
				}
			})
		}
	})

	t.Run("unknown section", func(t *testing.T) {
		_, err := parseSections([]string{"cluster", "foo"}, false)
		if err == nil {
			t.Fatal("parseSections: want error, got nil")
		}
		if !strings.Contains(err.Error(), `"foo"`) {
			t.Errorf("error %q doesn't name the unknown section", err)
		}
	})
}

func TestPrintStatus(t *testing.T) {
	members := []status.AgentInfo{
		agentInfo("rs0", "rs100:30100", status.RoleCtrl, true, true),
	}
	cfg := &config.Config{
		Storage: config.StorageConf{
			Type:       storage.Filesystem,
			Filesystem: &fs.Config{Path: "/opt/backups/pbm"},
		},
	}

	t.Run("section selection", func(t *testing.T) {
		tests := []struct {
			name       string
			sections   []string
			wantHdrs   []string
			wantPaths  []string
			wantAbsent []string
		}{
			{
				name:       "defaults",
				wantHdrs:   []string{"Cluster:", "PITR:", "Currently running:", "Backups:"},
				wantPaths:  []string{"/backup", "/config/main", "/status"},
				wantAbsent: []string{"Debug:"},
			},
			{
				name:       "backups only",
				sections:   []string{"backups"},
				wantHdrs:   []string{"Backups:"},
				wantPaths:  []string{"/backup", "/config/main"},
				wantAbsent: []string{"Cluster:"},
			},
			{
				name:       "debug only",
				sections:   []string{"debug"},
				wantHdrs:   []string{"Debug:"},
				wantPaths:  []string{"/status"},
				wantAbsent: []string{"Cluster:", "Backups:"},
			},
			{
				name:      "display order ignores flag order",
				sections:  []string{"debug", "backups", "running", "pitr", "cluster"},
				wantHdrs:  []string{"Cluster:", "PITR:", "Currently running:", "Backups:", "Debug:"},
				wantPaths: []string{"/backup", "/config/main", "/status"},
			},
			{
				name:     "not implemented sections",
				sections: []string{"pitr", "running"},
				wantHdrs: []string{"PITR:", "Currently running:"},
			},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				srv := newFakeAPI(t, members, cfg, nil)

				sections, err := parseSections(tc.sections, false)
				if err != nil {
					t.Fatalf("parseSections: %v", err)
				}

				var buf bytes.Buffer
				err = printStatus(context.Background(), &buf, apiclient.New([]string{srv.URL}), sections)
				if err != nil {
					t.Fatalf("printStatus: %v", err)
				}
				out := buf.String()

				pos := -1
				for _, h := range tc.wantHdrs {
					i := strings.Index(out, h)
					if i < 0 {
						t.Fatalf("missing %q in output:\n%s", h, out)
					}
					if i < pos {
						t.Errorf("%q is out of display order in output:\n%s", h, out)
					}
					pos = i
				}
				for _, h := range tc.wantAbsent {
					if strings.Contains(out, h) {
						t.Errorf("unexpected %q in output:\n%s", h, out)
					}
				}
				if got := srv.paths(); !slices.Equal(got, tc.wantPaths) {
					t.Errorf("requested paths = %v, want %v", got, tc.wantPaths)
				}
			})
		}
	})

	t.Run("pbm is not configured", func(t *testing.T) {
		srv := newFakeAPI(t, members, nil, nil)

		var buf bytes.Buffer
		err := printStatus(context.Background(), &buf, apiclient.New([]string{srv.URL}),
			map[string]bool{sectionBackups: true})
		if err != nil {
			t.Fatalf("printStatus: %v", err)
		}
		if !strings.Contains(buf.String(), "(not configured)") {
			t.Errorf("missing \"(not configured)\" in output:\n%s", buf.String())
		}
	})
}

func TestMemberHost(t *testing.T) {
	tests := []struct {
		name string
		m    status.AgentInfo
		want string
	}{
		{
			name: "attached to mongod",
			m: status.AgentInfo{
				Addr: "10.0.0.1", Port: 7946,
				MongoInfo: status.MongoInfo{Me: "rs100:30100"},
			},
			want: "rs100:30100",
		},
		{
			name: "without mongod",
			m:    status.AgentInfo{Addr: "10.0.0.1", Port: 7946},
			want: "10.0.0.1:7946",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := memberHost(tc.m); got != tc.want {
				t.Errorf("memberHost = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMemberState(t *testing.T) {
	tests := []struct {
		name string
		mi   status.MongoInfo
		want string
	}{
		{name: "without mongod", mi: status.MongoInfo{}, want: ""},
		{name: "primary", mi: status.MongoInfo{SetName: "rs0", IsPrimary: true}, want: " [P]"},
		{name: "secondary", mi: status.MongoInfo{SetName: "rs0", Secondary: true}, want: " [S]"},
		{
			name: "hidden secondary",
			mi:   status.MongoInfo{SetName: "rs0", Secondary: true, Hidden: true},
			want: " [H]",
		},
		{name: "arbiter", mi: status.MongoInfo{SetName: "rs0", ArbiterOnly: true}, want: " [A]"},
		{name: "unknown", mi: status.MongoInfo{SetName: "rs0"}, want: " [?]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := memberState(status.AgentInfo{MongoInfo: tc.mi}); got != tc.want {
				t.Errorf("memberState = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAgentLabel(t *testing.T) {
	tests := []struct {
		name     string
		role     status.Role
		isLeader bool
		want     string
	}{
		{name: "ctrl", role: status.RoleCtrl, want: "ctrl-agent"},
		{name: "ctrl leader", role: status.RoleCtrl, isLeader: true, want: "ctrl-agent-leader"},
		{name: "worker", role: status.RoleWorker, want: "worker-agent"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := agentLabel(status.AgentInfo{Role: tc.role, IsLeader: tc.isLeader})
			if got != tc.want {
				t.Errorf("agentLabel = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestAgentHealth(t *testing.T) {
	tests := []struct {
		name string
		m    status.AgentInfo
		want string
	}{
		{
			name: "ok",
			m:    status.AgentInfo{Alive: true, AgentStatus: status.SubsysStatus{OK: true}},
			want: "OK",
		},
		{
			name: "not alive",
			m:    status.AgentInfo{AgentStatus: status.SubsysStatus{OK: true}},
			want: "FAILED: agent is not alive",
		},
		{
			name: "agent error",
			m:    status.AgentInfo{Alive: true, AgentStatus: status.SubsysStatus{Err: "conn refused"}},
			want: "FAILED: conn refused",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := agentHealth(tc.m); got != tc.want {
				t.Errorf("agentHealth = %q, want %q", got, tc.want)
			}
		})
	}
}

// agentInfo builds a healthy agent attached to a mongod in setName.
func agentInfo(setName, me string, role status.Role, isLeader, isPrimary bool) status.AgentInfo {
	return status.AgentInfo{
		Role:        role,
		IsLeader:    isLeader,
		Version:     "2.14.0",
		Alive:       true,
		AgentStatus: status.SubsysStatus{OK: true},
		MongoInfo: status.MongoInfo{
			SetName:   setName,
			Me:        me,
			IsPrimary: isPrimary,
			Secondary: !isPrimary,
		},
	}
}

func sortedKeys(m map[string]bool) []string {
	return slices.Sorted(maps.Keys(m))
}

// fakeAPI is a ctrl-agent API stub that records the requested paths.
type fakeAPI struct {
	*httptest.Server

	mu   sync.Mutex
	seen map[string]bool
}

// newFakeAPI serves members, cfg and metas. A nil cfg answers 404 on /config/main.
func newFakeAPI(
	t *testing.T,
	members []status.AgentInfo,
	cfg *config.Config,
	metas []*backup.BackupMeta,
) *fakeAPI {
	t.Helper()

	f := &fakeAPI{seen: make(map[string]bool)}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.seen[r.URL.Path] = true
		f.mu.Unlock()

		var body any
		switch r.URL.Path {
		case "/status":
			body = members
		case "/config/" + config.DefaultConfigName:
			if cfg == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			body = cfg
		case "/backup":
			body = metas
		default:
			t.Errorf("unexpected path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(f.Close)

	return f
}

// paths returns the requested paths, sorted.
func (f *fakeAPI) paths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return sortedKeys(f.seen)
}
