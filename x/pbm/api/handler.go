package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

// NewRouter builds the web API routes.
func NewRouter(statusSvc *status.Svc) http.Handler {
	mux := http.NewServeMux()

	newStatusHandler(statusSvc).registerRoutes(mux)
	// newBackupHandler(backupSvc).registerRoutes(mux)

	return mux
}

// statusHandler serves cluster status endpoints.
type statusHandler struct {
	svc *status.Svc
}

func newStatusHandler(svc *status.Svc) *statusHandler {
	return &statusHandler{svc: svc}
}

func (h *statusHandler) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /status", h.handleStatus)
}

func (h *statusHandler) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, h.svc.GetAllMembers())
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("api: encode response: %v", err)
	}
}
