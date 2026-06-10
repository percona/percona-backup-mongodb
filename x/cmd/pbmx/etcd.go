package main

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
)

// runCtrlAgent starts the control agent
func runCtrlAgent(ctx context.Context, dataDir string) error {
	etcdSrv, err := startEmbeddedEtcd(ctx, dataDir)
	if err != nil {
		// An interrupt during startup cancels ctx, treat it as a clean shutdown.
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	defer etcdSrv.Close()

	select {
	case <-ctx.Done():
		return nil
	case err := <-etcdSrv.Err():
		return fmt.Errorf("embedded etcd stopped: %w", err)
	}
}

func startEmbeddedEtcd(ctx context.Context, dataDir string) (*embed.Etcd, error) {
	cfg := embed.NewConfig()
	if dataDir != "" {
		cfg.Dir = dataDir
	}

	etcdSrv, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, fmt.Errorf("start embedded etcd: %w", err)
	}

	etcdReadyTimeout := 60 * time.Second
	select {
	case <-etcdSrv.Server.ReadyNotify():
		return etcdSrv, nil
	case <-ctx.Done():
		// Interrupted before the server became ready: gracefully tear it down.
		etcdSrv.Close()
		return nil, ctx.Err()
	case <-time.After(etcdReadyTimeout):
		etcdSrv.Close()
		return nil, fmt.Errorf("embedded etcd not ready within %s", etcdReadyTimeout)
	}
}
