package backup

import (
	"encoding/json"
	"fmt"

	"github.com/percona/percona-backup-mongodb/x/pbm/defs"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage"
)

// StorageRepo reads backup metadata from the backup storage.
type StorageRepo struct {
	stg storage.Storage
}

// NewStorageRepo creates a storage-backed backup metadata reader.
func NewStorageRepo(stg storage.Storage) *StorageRepo {
	return &StorageRepo{
		stg: stg,
	}
}

// ListBackupMeta reads every backup metadata document from the storage.
// Unreadable metadata files are skipped.
func (r *StorageRepo) ListBackupMeta() ([]*BackupMeta, error) {
	backupFiles, err := r.stg.List("", defs.MetadataFileSuffix)
	if err != nil {
		return nil, errors.Wrap(err, "get a backups list from the storage")
	}

	backupMeta := make([]*BackupMeta, 0, len(backupFiles))
	for _, b := range backupFiles {
		meta, err := r.ReadMetadata(b.Name)
		if err != nil {
			fmt.Printf("read metadata of backup %s: %v\n", b.Name, err)
			continue
		}

		// todo: add file checks
		// err = backup.CheckBackupDataFiles(ctx, stg, meta)
		if err != nil {
			fmt.Printf("skip snapshot %s: %v\n", meta.Name, err)
			meta.Status = defs.StatusError
			meta.Err = err.Error()
		}

		backupMeta = append(backupMeta, meta)
	}

	return backupMeta, nil
}

// ReadMetadata reads and decodes a single backup metadata file from the storage.
func (r *StorageRepo) ReadMetadata(fname string) (*BackupMeta, error) {
	rdr, err := r.stg.SourceReader(fname)
	if err != nil {
		return nil, errors.Wrap(err, "open")
	}
	defer rdr.Close()

	var meta BackupMeta
	err = json.NewDecoder(rdr).Decode(&meta)
	if err != nil {
		return nil, errors.Wrap(err, "decode")
	}

	return &meta, nil
}
