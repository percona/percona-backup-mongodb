package api

import (
	"net/http"

	"github.com/percona/percona-backup-mongodb/x/pbm/backup"
)

// backupHandler serves PBM backup endpoints.
type backupHandler struct {
	repo *backup.Repo
}

func newBackupHandler(repo *backup.Repo) *backupHandler {
	return &backupHandler{repo: repo}
}

func (h *backupHandler) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /backup", h.handleGetAll)
}

func (h *backupHandler) handleGetAll(w http.ResponseWriter, r *http.Request) {
	metas, err := h.repo.GetAll(r.Context())
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, metas)
}
