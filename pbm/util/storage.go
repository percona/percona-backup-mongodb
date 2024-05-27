package util

import (
	"context"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/storage/azure"
	"github.com/percona/percona-backup-mongodb/pbm/storage/fs"
	"github.com/percona/percona-backup-mongodb/pbm/storage/s3"
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
