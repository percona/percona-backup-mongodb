package etcd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
)

// Config holds embedded etcd server configuration.
type Config struct {
	DataDir string

	// agent listen etcd peer on this port
	ListenPeerPort int
	// agent listen etcd client on this port
	ListenClientPort int

	// agent advertise this url for communication with peer, empty keeps the localhost default.
	AdvertisePeerURL string
	// agent advertise this url for communication with client, empty keeps the localhost default.
	AdvertiseClientURL string

	// InitialCluster is the comma-separated bootstrap member list:
	//   "etcd-0=http://etcd-0...:2380,etcd-1=http://etcd-1...:2380,..."
	// When empty it is derived from the member name and AdvertisePeerURL,
	// yielding a single-node cluster.
	InitialCluster string
}

// readyTimeout bounds how long Start waits for the server to become ready.
const readyTimeout = 60 * time.Second

// Server owns an embedded etcd server instance.
type Server struct {
	srv *embed.Etcd
}

// Start brings up the embedded etcd server under the given member name and
// blocks until it is ready, ctx is canceled, or readyTimeout elapses.
func Start(ctx context.Context, name string, cfg Config) (*Server, error) {
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

	srv, err := embed.StartEtcd(ecfg)
	if err != nil {
		return nil, fmt.Errorf("start embedded etcd: %w", err)
	}

	select {
	case <-srv.Server.ReadyNotify():
		return &Server{srv: srv}, nil
	case <-ctx.Done():
		// Interrupted before the server became ready: gracefully tear it down.
		srv.Close()
		return nil, ctx.Err()
	case <-time.After(readyTimeout):
		srv.Close()
		return nil, fmt.Errorf("embedded etcd not ready within %s", readyTimeout)
	}
}

// IsLeader reports whether this node currently holds etcd leadership.
func (s *Server) IsLeader() bool {
	return s.srv.Server.Leader() == s.srv.Server.MemberID()
}

// Err returns fatal server errors after a successful Start.
func (s *Server) Err() <-chan error {
	return s.srv.Err()
}

// Close gracefully stops the embedded etcd server.
func (s *Server) Close() {
	s.srv.Close()
}

func listenURL(port int) url.URL {
	return url.URL{Scheme: "http", Host: fmt.Sprintf("0.0.0.0:%d", port)}
}
