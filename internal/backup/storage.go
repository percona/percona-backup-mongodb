package backup

import (
	"context"
	"encoding/json"
	"path"

	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/internal/archive"
	"github.com/percona/percona-backup-mongodb/internal/config"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/storage"
	"github.com/percona/percona-backup-mongodb/internal/util"
	"github.com/percona/percona-backup-mongodb/internal/version"
)

type StorageManager interface {
	GetAllBackups(ctx context.Context) ([]BackupMeta, error)
	GetBackupByName(ctx context.Context, name string) (*BackupMeta, error)
}

type storageManagerImpl struct {
	cfg config.StorageConf
	stg storage.Storage
}

func NewStorageManager(cfg config.StorageConf) (*storageManagerImpl, error) {
	stg, err := util.StorageFromConfig(cfg, nil)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get backup store")
	}

	_, err = stg.FileStat(defs.StorInitFile)
	if !errors.Is(err, storage.ErrNotExist) {
		return nil, err
	}

	return &storageManagerImpl{cfg: cfg, stg: stg}, nil
}

func (m *storageManagerImpl) GetAllBackups(ctx context.Context) ([]BackupMeta, error) {
	l := log.LogEventFromContext(ctx)

	bcpList, err := m.stg.List("", defs.MetadataFileSuffix)
	if err != nil {
		return nil, errors.Wrap(err, "get a backups list from the storage")
	}
	l.Debug("got backups list: %v", len(bcpList))

	var rv []BackupMeta
	for _, b := range bcpList {
		l.Debug("bcp: %v", b.Name)

		d, err := m.stg.SourceReader(b.Name)
		if err != nil {
			return nil, errors.Wrapf(err, "read meta for %v", b.Name)
		}

		v := BackupMeta{}
		err = json.NewDecoder(d).Decode(&v)
		d.Close()
		if err != nil {
			return nil, errors.Wrapf(err, "unmarshal backup meta [%s]", b.Name)
		}

		err = CheckBackupFiles(ctx, &v, m.stg)
		if err != nil {
			l.Warning("skip snapshot %s: %v", v.Name, err)
			v.Status = defs.StatusError
			v.Err = err.Error()
		}
		rv = append(rv, v)
	}

	return rv, nil
}

func (m *storageManagerImpl) GetBackupByName(ctx context.Context, name string) (*BackupMeta, error) {
	l := log.LogEventFromContext(ctx)
	l.Debug("get backup by name: %v", name)

	rdr, err := m.stg.SourceReader(name + defs.MetadataFileSuffix)
	if err != nil {
		return nil, errors.Wrapf(err, "read meta for %v", name)
	}
	defer rdr.Close()

	v := &BackupMeta{}
	if err := json.NewDecoder(rdr).Decode(&v); err != nil {
		return nil, errors.Wrapf(err, "unmarshal backup meta [%s]", name)
	}

	if err := CheckBackupFiles(ctx, v, m.stg); err != nil {
		l.Warning("no backup files %s: %v", v.Name, err)
		v.Status = defs.StatusError
		v.Err = err.Error()
	}

	return v, nil
}

func CheckBackupFiles(ctx context.Context, bcp *BackupMeta, stg storage.Storage) error {
	// !!! TODO: Check physical files ?
	if bcp.Type != defs.LogicalBackup {
		return nil
	}

	legacy := version.IsLegacyArchive(bcp.PBMVersion)
	eg, _ := errgroup.WithContext(ctx)
	for _, rs := range bcp.Replsets {
		rs := rs

		eg.Go(func() error { return checkFile(stg, rs.DumpName) })
		eg.Go(func() error { return checkFile(stg, rs.OplogName) })

		if legacy {
			continue
		}

		nss, err := ReadArchiveNamespaces(stg, rs.DumpName)
		if err != nil {
			return errors.Wrapf(err, "parse metafile %q", rs.DumpName)
		}

		for _, ns := range nss {
			if ns.Size == 0 {
				continue
			}

			ns := archive.NSify(ns.Database, ns.Collection)
			f := path.Join(bcp.Name, rs.Name, ns+bcp.Compression.Suffix())

			eg.Go(func() error { return checkFile(stg, f) })
		}
	}

	return eg.Wait()
}

func ReadArchiveNamespaces(stg storage.Storage, metafile string) ([]*archive.Namespace, error) {
	r, err := stg.SourceReader(metafile)
	if err != nil {
		return nil, errors.Wrapf(err, "open %q", metafile)
	}
	defer r.Close()

	meta, err := archive.ReadMetadata(r)
	if err != nil {
		return nil, errors.Wrapf(err, "parse metafile %q", metafile)
	}

	return meta.Namespaces, nil
}

func checkFile(stg storage.Storage, filename string) error {
	f, err := stg.FileStat(filename)
	if err != nil {
		return errors.Wrapf(err, "file %q", filename)
	}
	if f.Size == 0 {
		return errors.Errorf("%q is empty", filename)
	}

	return nil
}
