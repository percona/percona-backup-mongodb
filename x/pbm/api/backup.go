package api

import (
	"net/http"

	"github.com/percona/percona-backup-mongodb/x/pbm/backup"
)

// backupHandler serves PBM backup endpoints.
type backupHandler struct {
	repo    *backup.Repo
	physSvc *backup.PhysSvc
}

func newBackupHandler(repo *backup.Repo, physSvc *backup.PhysSvc) *backupHandler {
	return &backupHandler{repo: repo, physSvc: physSvc}
}

func (h *backupHandler) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /backup", h.handleGetAll)
	mux.HandleFunc("POST /backup", h.handleBackup)
}

func (h *backupHandler) handleGetAll(w http.ResponseWriter, r *http.Request) {
	metas, err := h.repo.GetAll(r.Context())
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, metas)
}

func (h *backupHandler) handleBackup(w http.ResponseWriter, r *http.Request) {
	t, err := h.physSvc.Start(r.Context())
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusAccepted, t)
}
