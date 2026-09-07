package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/percona/percona-backup-mongodb/x/pbm/backup"
	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

// NewRouter builds the web API routes.
func NewRouter(statusSvc *status.Svc, configSvc *config.Svc, backupRepo *backup.Repo) http.Handler {
	mux := http.NewServeMux()

	newStatusHandler(statusSvc).registerRoutes(mux)
	newConfigHandler(configSvc).registerRoutes(mux)
	newBackupHandler(backupRepo).registerRoutes(mux)

	return leaderOnly(statusSvc, mux)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("api: encode response: %v", err)
	}
}
