package config

import (
	"context"
	"encoding/json"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
)

const (
	DefaultConfigName = "main"
	keyPrefix         = "/pbm/config/"
)

var ErrNotFound = errors.New("config not found")

// storageResyncer rebuilds the backup metadata list so it matches the given
// storage.
type storageResyncer interface {
	Resync(ctx context.Context, stg *StorageConf) error
}

// Svc is the configuration service.
// It manages PBM's configuration documents persisted in etcd.
type Svc struct {
	ccDB     *clientv3.Client
	resyncer storageResyncer
}

// New creates a config service instance.
func New(ccDB *clientv3.Client, resyncer storageResyncer) *Svc {
	return &Svc{ccDB: ccDB, resyncer: resyncer}
}

// Get returns the config document under the given name, defaulting to
// 'main' when empty. It returns ErrNotFound if no such document
// exists.
func (s *Svc) Get(ctx context.Context, name string) (*Config, error) {
	resp, err := s.ccDB.Get(ctx, key(name))
	if err != nil {
		return nil, errors.Wrap(err, "get config")
	}
	if len(resp.Kvs) == 0 {
		return nil, ErrNotFound
	}

	cfg := &Config{}
	if err := json.Unmarshal(resp.Kvs[0].Value, cfg); err != nil {
		return nil, errors.Wrap(err, "unmarshal config")
	}

	return cfg, nil
}

// GetAll returns every stored config document, ordered by name ascending.
// It returns an empty slice when no config documents exist.
func (s *Svc) GetAll(ctx context.Context) ([]*Config, error) {
	resp, err := s.ccDB.Get(ctx, keyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Wrap(err, "get configs")
	}

	out := make([]*Config, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		cfg := &Config{}
		if err := json.Unmarshal(kv.Value, cfg); err != nil {
			return nil, errors.Wrapf(err, "unmarshal config %q", string(kv.Key))
		}
		out = append(out, cfg)
	}

	return out, nil
}

// Save persists the config and keeps the backup list in sync with its storage.
func (s *Svc) Save(ctx context.Context, cfg *Config) error {
	if cfg.Name == "" {
		cfg.Name = DefaultConfigName
	}

	oldCfg, err := s.Get(ctx, cfg.Name)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return errors.Wrap(err, "get current config")
	}

	if err := s.Upsert(ctx, cfg); err != nil {
		return err
	}

	// if ther's no previous config, or the storage instance changed, do resync
	if oldCfg == nil || !oldCfg.IsSameStorage(cfg) {
		if err := s.resyncer.Resync(ctx, &cfg.Storage); err != nil {
			return errors.Wrap(err, "sync backup list")
		}
	}

	return nil
}

// Resync rebuilds the backup metadata list to match the storage of the named
// config, defaulting to DefaultConfigName when empty.
func (s *Svc) Resync(ctx context.Context, name string) error {
	cfg, err := s.Get(ctx, name)
	if err != nil {
		return err
	}

	if err := s.resyncer.Resync(ctx, &cfg.Storage); err != nil {
		return errors.Wrap(err, "sync backup list")
	}

	return nil
}

// Upsert stores a config document, creating it when absent and replacing it in
// full when present. The document is identified by cfg.Name, defaulting to
// DefaultConfigName when empty.
func (s *Svc) Upsert(ctx context.Context, cfg *Config) error {
	if cfg.Name == "" {
		cfg.Name = DefaultConfigName
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return errors.Wrap(err, "marshal config")
	}

	if _, err := s.ccDB.Put(ctx, key(cfg.Name), string(data)); err != nil {
		return errors.Wrap(err, "put config")
	}

	return nil
}

// Delete removes the config document under the given name, defaulting to
// DefaultConfigName when empty.
func (s *Svc) Delete(ctx context.Context, name string) error {
	resp, err := s.ccDB.Delete(ctx, key(name))
	if err != nil {
		return errors.Wrap(err, "delete config")
	}
	if resp.Deleted == 0 {
		return ErrNotFound
	}

	return nil
}

// key resolves the config name to its etcd key.
func key(name string) string {
	if name == "" {
		name = DefaultConfigName
	}
	return keyPrefix + name
}
