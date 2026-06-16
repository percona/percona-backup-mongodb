package pbm

import (
	"context"
	"log"
)

// RunWorkerAgent starts the worker agent: it performs backup/restore work.
func RunWorkerAgent(ctx context.Context, cfg *WorkerAgentConfig) error {
	disco, err := startDiscovery(cfg.Name, cfg.DiscoConfig)
	if err != nil {
		// An interrupt during startup cancels ctx, treat it as a clean shutdown.
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer func() {
		if err := disco.stop(); err != nil {
			log.Printf("serf shutdown: %v", err)
		}
	}()
	log.Printf("agent: %s added to PBM cluster", cfg.Name)

	<-ctx.Done()

	log.Printf("agent: %s is shutdown", cfg.Name)
	return nil
}
