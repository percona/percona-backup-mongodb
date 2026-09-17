package factory

import (
	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage/fs"
)

// ErrStorageUndefined is an error for undefined storage
var ErrStorageUndefined = errors.New("storage undefined")

// StorageFromConfig creates and returns a storage object based on a given PBM's config (storage section).
func Create(cfg *config.StorageConf) (storage.Storage, error) {
	switch cfg.Type {
	case storage.Filesystem:
		return fs.New(cfg.Filesystem)
	case storage.Undefined:
		return nil, ErrStorageUndefined
	default:
		return nil, errors.Errorf("unknown storage type %s", cfg.Type)
	}
}
