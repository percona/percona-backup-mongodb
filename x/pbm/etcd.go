package pbm

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
)

func startEmbeddedEtcd(ctx context.Context, name string, cfg EtcdConfig) (*embed.Etcd, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create etcd data dir: %w", err)
	}

	ecfg := embed.NewConfig()
	ecfg.Dir = cfg.DataDir
	ecfg.Name = name
	ecfg.LogOutputs = []string{filepath.Join(cfg.DataDir, fmt.Sprintf("etcd-%s.log", name))}

	ecfg.InitialClusterToken = "pbm-cc-cluster"
	ecfg.ClusterState = embed.ClusterStateFlagNew

	if cfg.ListenClientPort != 0 {
		ecfg.ListenClientUrls = []url.URL{listenURL(cfg.ListenClientPort)}
	}
	if cfg.ListenPeerPort != 0 {
		ecfg.ListenPeerUrls = []url.URL{listenURL(cfg.ListenPeerPort)}
	}

	if cfg.AdvertiseClientURL != "" {
		u, err := url.Parse(cfg.AdvertiseClientURL)
		if err != nil {
			return nil, fmt.Errorf("parse advertise client url: %w", err)
		}
		ecfg.AdvertiseClientUrls = []url.URL{*u}
	}
	if cfg.AdvertisePeerURL != "" {
		u, err := url.Parse(cfg.AdvertisePeerURL)
		if err != nil {
			return nil, fmt.Errorf("parse advertise peer url: %w", err)
		}
		ecfg.AdvertisePeerUrls = []url.URL{*u}
	}

	// Bootstrap membership: an explicit member list wins; otherwise derive a
	// single-node cluster from this member's name and advertised peer URL.
	if cfg.InitialCluster != "" {
		ecfg.InitialCluster = cfg.InitialCluster
	} else {
		ecfg.InitialCluster = ecfg.InitialClusterFromName(ecfg.Name)
	}

	etcdSrv, err := embed.StartEtcd(ecfg)
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

// listenURL builds an http bind URL on 0.0.0.0 for the given port.
func listenURL(port int) url.URL {
	return url.URL{Scheme: "http", Host: fmt.Sprintf("0.0.0.0:%d", port)}
}
