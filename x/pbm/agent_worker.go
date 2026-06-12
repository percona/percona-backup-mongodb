package pbm

import "context"

// RunWorkerAgent starts the worker agent: it performs backup/restore work.
func RunWorkerAgent(ctx context.Context, cfg *WorkerAgentConfig) error {
	<-ctx.Done()
	return nil
}
