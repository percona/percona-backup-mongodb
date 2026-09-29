package api

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/percona/percona-backup-mongodb/x/pbm/backup"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
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
	opts := backup.Options{}
	if err := json.NewDecoder(r.Body).Decode(&opts); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "invalid backup options", http.StatusBadRequest)
		return
	}

	t, err := h.physSvc.Start(r.Context(), opts)
	if err != nil {
		if errors.Is(err, backup.ErrInvalidOptions) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusAccepted, t)
}
