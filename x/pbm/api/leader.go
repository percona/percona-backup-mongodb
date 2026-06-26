package api

import (
	"net"
	"net/http"
	"strconv"

	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

// routingResponse tells a client which ctrl-agent currently leads, so it can
// retry the request against the leader.
type routingResponse struct {
	Leader leaderRef `json:"leader"`
}

type leaderRef struct {
	Name    string `json:"name"`
	Address string `json:"address"`
}

// leaderRouter reports whether the local node leads and locates the current
// leader. *status.Svc satisfies it.
type leaderRouter interface {
	IsLeader() bool
	Leader() (status.AgentInfo, bool)
}

// leaderOnly is middleware which filter out requests on non-leader agents.
// Non-leader replies 421 Misdirected Request with the leader's routing
// info. While leader is unknown, it replies 503.
// If agent is leader, request is processed with appropriate handler.
func leaderOnly(svc leaderRouter, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if svc.IsLeader() {
			next.ServeHTTP(w, r)
			return
		}

		leader, ok := svc.Leader()
		if !ok || leader.APIPort == 0 {
			http.Error(w, "no leader elected yet", http.StatusServiceUnavailable)
			return
		}

		writeJSON(w, http.StatusMisdirectedRequest, routingResponse{
			Leader: leaderRef{
				Name:    leader.Name,
				Address: net.JoinHostPort(leader.Addr, strconv.Itoa(leader.APIPort)),
			},
		})
	})
}
