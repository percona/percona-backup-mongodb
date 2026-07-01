package pbm

import (
	"context"
	"fmt"
	"log"

	"github.com/percona/percona-backup-mongodb/x/pbm/connect"
	"github.com/percona/percona-backup-mongodb/x/pbm/disco"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

// RunWorkerAgent starts the worker agent: it performs backup/restore work.
func RunWorkerAgent(ctx context.Context, cfg *WorkerAgentConfig) error {
	mc, err := connect.ConnectDirect(ctx, cfg.MongoURI)
	if err != nil {
		return fmt.Errorf("connect local mongod: %w", err)
	}
	defer connect.Disconnect(mc)

	svc := status.NewForWorkerAgent(cfg.Name, mc)

	d, err := disco.Start(ctx, cfg.Name, cfg.Config, svc.DiscoSync())
	if err != nil {
		return fmt.Errorf("start pbm cluster: %w", err)
	}
	defer func() {
		if err := d.Stop(); err != nil {
			log.Printf("serf shutdown: %v", err)
		}
	}()
	log.Printf("agent: %s added to PBM cluster", cfg.Name)

	svc.SetPublisher(d)
	go svc.Run(ctx)

	<-ctx.Done()

	log.Printf("agent: %s is shutdown", cfg.Name)
	return nil
}
