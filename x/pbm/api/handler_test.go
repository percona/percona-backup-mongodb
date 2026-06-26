package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

// fakeRouter is a static leaderRouter for exercising the leaderOnly middleware.
type fakeRouter struct {
	leader   bool
	member   status.AgentInfo
	memberOK bool
}

func (f fakeRouter) IsLeader() bool                   { return f.leader }
func (f fakeRouter) Leader() (status.AgentInfo, bool) { return f.member, f.memberOK }

// okHandler is the protected handler; it must run only on the leader.
var okHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("served"))
})

func TestLeaderOnlyServesOnLeader(t *testing.T) {
	h := leaderOnly(fakeRouter{leader: true}, okHandler)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/status", nil))

	if rr.Code != http.StatusOK || rr.Body.String() != "served" {
		t.Fatalf("leader should serve: code=%d body=%q", rr.Code, rr.Body.String())
	}
}

func TestLeaderOnlyRoutesToLeader(t *testing.T) {
	h := leaderOnly(fakeRouter{
		leader:   false,
		memberOK: true,
		member:   status.AgentInfo{Name: "ctrl-agent-0", Addr: "10.0.0.1", APIPort: 9000},
	}, okHandler)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/status", nil))

	if rr.Code != http.StatusMisdirectedRequest {
		t.Fatalf("non-leader should return 421, got %d", rr.Code)
	}
	var got routingResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if got.Leader.Name != "ctrl-agent-0" || got.Leader.Address != "10.0.0.1:9000" {
		t.Fatalf("unexpected routing info: %+v", got.Leader)
	}
}

func TestLeaderOnlyNoLeaderYet(t *testing.T) {
	// Non-leader and no leader known, or leader without an API port → 503.
	for name, fr := range map[string]fakeRouter{
		"unknown":    {leader: false, memberOK: false},
		"no apiport": {leader: false, memberOK: true, member: status.AgentInfo{Name: "ctrl-agent-0", Addr: "10.0.0.1"}},
	} {
		t.Run(name, func(t *testing.T) {
			h := leaderOnly(fr, okHandler)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/status", nil))
			if rr.Code != http.StatusServiceUnavailable {
				t.Fatalf("expected 503, got %d", rr.Code)
			}
		})
	}
}
