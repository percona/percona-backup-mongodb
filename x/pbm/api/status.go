package api

import (
	"net/http"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

// statusHandler serves cluster status endpoints.
type statusHandler struct {
	svc *status.Svc
}

func newStatusHandler(svc *status.Svc) *statusHandler {
	return &statusHandler{svc: svc}
}

func (h *statusHandler) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /status", h.handleStatus)
	mux.HandleFunc("GET /status/rs/{rs}", h.handleRSStatus)
	mux.HandleFunc("GET /status/agent/{agent}", h.handleAgentStatus)
}

func (h *statusHandler) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.svc.GetAllMembers())
}

func (h *statusHandler) handleRSStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.svc.GetMembersForRS(r.PathValue("rs")))
}

func (h *statusHandler) handleAgentStatus(w http.ResponseWriter, r *http.Request) {
	member, err := h.svc.GetMember(r.PathValue("agent"))
	if err != nil {
		if errors.Is(err, status.ErrNotFound) {
			http.Error(w, "agent not found", http.StatusNotFound)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, member)
}
