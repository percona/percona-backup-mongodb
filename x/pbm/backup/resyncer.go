package backup

import (
	"context"
	"fmt"

	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage/factory"
)

// StorageResyncer rebuilds and persists the backup metadata list from a storage.
type StorageResyncer struct {
	repo *Repo
}

// NewStorageResyncer creates a resyncer that rebuilds repo's metadata index.
func NewStorageResyncer(repo *Repo) *StorageResyncer {
	return &StorageResyncer{repo: repo}
}

// Resync builds the storage described by config and rebuilds its backup metadata.
func (r *StorageResyncer) Resync(ctx context.Context, stg *config.StorageConf) error {
	newStg, err := factory.Create(stg)
	if err != nil {
		return errors.Wrap(err, "create storage")
	}

	return syncBackupList(ctx, r.repo, NewStorageRepo(newStg))
}

// syncBackupList rebuilds the backups metadata stored within etcd from the storage.
// It drops all metadata and re-inserts one document per backup found on the backup storage.
func syncBackupList(ctx context.Context, repo *Repo, storageRepo *StorageRepo) error {
	// todo: add events:
	// 	l.Info("syncing backup list for main storage")
	// 	l.Info("syncing backup list for profile %q", profile)

	cntDeleted, err := repo.DeleteAll(ctx)
	if err != nil {
		return errors.Wrapf(err, "clear backup list")
	}
	fmt.Printf("deleted %d backup metadata docs\n", cntDeleted)

	backupList, err := storageRepo.ListBackupMeta()
	if err != nil {
		return errors.Wrap(err, "get all backups meta from the storage")
	}

	fmt.Printf("got backups list: %d\n", len(backupList))

	if len(backupList) == 0 {
		return nil
	}

	for _, backupMeta := range backupList {
		fmt.Printf("backup: %s, size=%d\n", backupMeta.Name, backupMeta.Size)
		err = repo.Insert(ctx, backupMeta)
		if err != nil {
			return errors.Wrapf(err, "insert backup meta %q", backupMeta.Name)
		}
	}

	return nil
}
