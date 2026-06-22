package pbm

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

// RunCtrlAgent starts the control agent: it joins the PBM discovery cluster,
// brings up embedded etcd, and blocks until ctx is canceled or the server
// reports a fatal error.
func RunCtrlAgent(ctx context.Context, cfg *CtrlAgentConfig) error {
	statusSvc := status.New(cfg.Name, status.RoleCtrl, cfg.MongoURI)

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

	statusSvc.SetPublisher(disco)
	go statusSvc.Run(ctx)

	etcdSrv, err := startEmbeddedEtcd(ctx, cfg.Name, cfg.EtcdConfig)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("start etcd: %w", err)
	}
	defer etcdSrv.Close()
	log.Printf("ctrl-agent %s started control collection db", cfg.Name)

	select {
	case <-ctx.Done():
		log.Printf("agent: %s is shutdown", cfg.Name)
		return nil
	case err := <-etcdSrv.Err():
		return fmt.Errorf("embedded etcd stopped: %w", err)
	}
}
