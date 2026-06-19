package pbm

import (
	"context"
	"fmt"
	"log"
)

// RunWorkerAgent starts the worker agent: it performs backup/restore work.
func RunWorkerAgent(ctx context.Context, cfg *WorkerAgentConfig) error {
	disco, err := startDiscovery(ctx, cfg.Name, cfg.DiscoConfig)
	if err != nil {
		return fmt.Errorf("start pbm cluster: %w", err)
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
