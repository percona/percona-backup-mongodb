package api

import (
	"encoding/json"
	"net/http"

	"go.uber.org/zap"

	"github.com/percona/percona-backup-mongodb/x/pbm/backup"
	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

// NewRouter builds the web API routes.
func NewRouter(
	lg *zap.Logger,
	statusSvc *status.Svc,
	configSvc *config.Svc,
	backupRepo *backup.Repo,
	physSvc *backup.PhysSvc,
) http.Handler {
	lg = lg.Named("api")
	mux := http.NewServeMux()

	newStatusHandler(lg, statusSvc).registerRoutes(mux)
	newConfigHandler(lg, configSvc).registerRoutes(mux)
	newBackupHandler(lg, backupRepo, physSvc).registerRoutes(mux)

	return leaderOnly(lg, statusSvc, mux)
}

func writeJSON(lg *zap.Logger, w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		lg.Warn("encode response", zap.Error(err))
	}
}
