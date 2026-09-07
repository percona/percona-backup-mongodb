package api

import (
	"encoding/json"
	"net/http"

	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
)

// configHandler serves PBM config endpoints.
type configHandler struct {
	svc *config.Svc
}

func newConfigHandler(svc *config.Svc) *configHandler {
	return &configHandler{svc: svc}
}

func (h *configHandler) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /config", h.handleGetAll)
	mux.HandleFunc("GET /config/{name}", h.handleGet)
	mux.HandleFunc("PUT /config/{name}", h.handleUpsert)
	mux.HandleFunc("DELETE /config/{name}", h.handleDelete)
	mux.HandleFunc("POST /config/{name}/resync", h.handleResync)
}

func (h *configHandler) handleGetAll(w http.ResponseWriter, r *http.Request) {
	cfgs, err := h.svc.GetAll(r.Context())
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, cfgs)
}

func (h *configHandler) handleGet(w http.ResponseWriter, r *http.Request) {
	cfg, err := h.svc.Get(r.Context(), r.PathValue("name"))
	if err != nil {
		if errors.Is(err, config.ErrNotFound) {
			http.Error(w, "config not found", http.StatusNotFound)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, cfg)
}

func (h *configHandler) handleUpsert(w http.ResponseWriter, r *http.Request) {
	cfg := &config.Config{}
	if err := json.NewDecoder(r.Body).Decode(cfg); err != nil {
		http.Error(w, "invalid config document", http.StatusBadRequest)
		return
	}
	// The path is authoritative for the config id.
	cfg.Name = r.PathValue("name")

	if err := h.svc.Save(r.Context(), cfg); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *configHandler) handleResync(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Resync(r.Context(), r.PathValue("name")); err != nil {
		if errors.Is(err, config.ErrNotFound) {
			http.Error(w, "config not found", http.StatusNotFound)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *configHandler) handleDelete(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Delete(r.Context(), r.PathValue("name")); err != nil {
		if errors.Is(err, config.ErrNotFound) {
			http.Error(w, "config not found", http.StatusNotFound)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
