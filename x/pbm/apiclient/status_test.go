package apiclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/percona/percona-backup-mongodb/x/pbm/api"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

// writeMembers encodes a /status array response.
func writeMembers(w http.ResponseWriter, members []status.AgentInfo) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(members)
}

// writeRouting encodes a 421 leader redirect to addr.
func writeRouting(w http.ResponseWriter, addr string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusMisdirectedRequest)
	_ = json.NewEncoder(w).Encode(api.RoutingResponse{
		Leader: api.LeaderRef{Name: "ctrl-0", Address: addr},
	})
}

func TestStatusLeaderServes(t *testing.T) {
	want := []status.AgentInfo{{Name: "ctrl-0", IsLeader: true}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		writeMembers(w, want)
	}))
	defer srv.Close()

	got, err := New([]string{srv.URL}).Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(got) != 1 || got[0].Name != "ctrl-0" {
		t.Fatalf("unexpected members: %+v", got)
	}
}

func TestStatusFollowsLeaderRedirect(t *testing.T) {
	want := []status.AgentInfo{{Name: "leader", IsLeader: true}}
	leader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeMembers(w, want)
	}))
	defer leader.Close()

	leaderAddr := strings.TrimPrefix(leader.URL, "http://")
	follower := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeRouting(w, leaderAddr)
	}))
	defer follower.Close()

	got, err := New([]string{follower.URL}).Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(got) != 1 || got[0].Name != "leader" {
		t.Fatalf("unexpected members: %+v", got)
	}
}

func TestStatusFallsThroughToWorkingEndpoint(t *testing.T) {
	// First endpoint has no leader (503); second serves.
	noLeader := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no leader elected yet", http.StatusServiceUnavailable)
	}))
	defer noLeader.Close()

	want := []status.AgentInfo{{Name: "ctrl-1", IsLeader: true}}
	working := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeMembers(w, want)
	}))
	defer working.Close()

	// Include an unreachable endpoint too, to exercise transport-error fallthrough.
	got, err := New([]string{"127.0.0.1:1", noLeader.URL, working.URL}).Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(got) != 1 || got[0].Name != "ctrl-1" {
		t.Fatalf("unexpected members: %+v", got)
	}
}

func TestStatusRedirectLoopGuard(t *testing.T) {
	// Server always replies 421, even after the redirect: must error, not loop.
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeRouting(w, strings.TrimPrefix(srv.URL, "http://"))
	}))
	defer srv.Close()

	_, err := New([]string{srv.URL}).Status(context.Background())
	if err == nil {
		t.Fatal("expected error on redirect loop, got nil")
	}
}

func TestStatusNoEndpoints(t *testing.T) {
	_, err := New(nil).Status(context.Background())
	if err == nil {
		t.Fatal("expected error with no endpoints, got nil")
	}
}
