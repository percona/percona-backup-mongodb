package util

import (
	"context"

	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/storage"
	"github.com/percona/percona-backup-mongodb/internal/storage/azure"
	"github.com/percona/percona-backup-mongodb/internal/storage/blackhole"
	"github.com/percona/percona-backup-mongodb/internal/storage/fs"
	"github.com/percona/percona-backup-mongodb/internal/storage/s3"
)

// ErrStorageUndefined is an error for undefined storage
var ErrStorageUndefined = errors.New("storage undefined")

// StorageFromConfig creates and returns a storage object based on a given config
func StorageFromConfig(cfg config.StorageConf, l log.LogEvent) (storage.Storage, error) {
	switch cfg.Type {
	case storage.S3:
		return s3.New(cfg.S3, l)
	case storage.Azure:
		return azure.New(cfg.Azure, l)
	case storage.Filesystem:
		return fs.New(cfg.Filesystem)
	case storage.BlackHole:
		return blackhole.New(), nil
	case storage.Undef:
		return nil, ErrStorageUndefined
	default:
		return nil, errors.Errorf("unknown storage type %s", cfg.Type)
	}
}

// GetStorage reads current storage config and creates and
// returns respective storage.Storage object
func GetStorage(ctx context.Context, m connect.Client, l log.LogEvent) (storage.Storage, error) {
	c, err := config.GetConfig(ctx, m)
	if err != nil {
		return nil, errors.Wrap(err, "get config")
	}

	return StorageFromConfig(c.Storage, l)
}
