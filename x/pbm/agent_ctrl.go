package pbm

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/percona/percona-backup-mongodb/x/pbm/api"
	"github.com/percona/percona-backup-mongodb/x/pbm/connect"
	"github.com/percona/percona-backup-mongodb/x/pbm/etcd"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

// RunCtrlAgent starts the control agent: it joins the PBM discovery cluster,
// brings up embedded etcd, and blocks until ctx is canceled or the server
// reports a fatal error.
func RunCtrlAgent(ctx context.Context, cfg *CtrlAgentConfig) error {
	mc, err := connect.ConnectDirect(ctx, cfg.MongoURI)
	if err != nil {
		return fmt.Errorf("connect local mongod: %w", err)
	}
	defer connect.Disconnect(mc)

	statusSvc := status.NewForCtrlAgent(cfg.Name, mc, cfg.APISrvPort)

	disco, err := startDiscovery(ctx, cfg.Name, cfg.DiscoConfig, statusSvc.DiscoSync())
	if err != nil {
		return fmt.Errorf("start pbm cluster: %w", err)
	}
	defer func() {
		if err := disco.stop(); err != nil {
			log.Printf("serf shutdown: %v", err)
		}
	}()
	log.Printf("ctrl-agent: %s added to PBM cluster", cfg.Name)

	etcdSrv, err := etcd.Start(ctx, cfg.Name, cfg.Config)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("start etcd: %w", err)
	}
	defer etcdSrv.Close()
	log.Printf("ctrl-agent %s started control collection db", cfg.Name)

	// wire the status service before starting its loop
	statusSvc.SetPublisher(disco)
	statusSvc.SetLeaderChecker(etcdSrv)
	go statusSvc.Run(ctx)

	apiSrv := api.Start(api.Config{Port: cfg.APISrvPort}, api.NewRouter(statusSvc))
	log.Printf("ctrl-agent %s started REST API on port %d", cfg.Name, cfg.APISrvPort)

	select {
	case <-ctx.Done():
		log.Printf("agent: %s is shutdown", cfg.Name)
		return nil
	case err := <-etcdSrv.Err():
		return fmt.Errorf("embedded etcd stopped: %w", err)
	case err := <-apiSrv.Err():
		return fmt.Errorf("api server stopped: %w", err)
	}
}
