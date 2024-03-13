package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"path"

	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm/archive"
	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/util"
	"github.com/percona/percona-backup-mongodb/pbm/version"
)

type StorageManager interface {
	GetAllBackups(ctx context.Context) ([]BackupMeta, error)
	GetBackupByName(ctx context.Context, name string) (*BackupMeta, error)
}

type storageManagerImpl struct {
	cfg config.StorageConf
	stg storage.Storage
}

func NewStorageManager(ctx context.Context, cfg config.StorageConf) (*storageManagerImpl, error) {
	stg, err := util.StorageFromConfig(cfg, log.LogEventFromContext(ctx))
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

		eg.Go(func() error {
			if version.IsLegacyBackupOplog(bcp.PBMVersion) {
				return checkFile(stg, rs.OplogName)
			}

			files, err := stg.List(rs.OplogName, "")
			if err != nil {
				return errors.Wrap(err, "list")
			}
			if len(files) == 0 {
				return errors.Wrap(err, "no oplog files")
			}
			for i := range files {
				if files[i].Size == 0 {
					return errors.Errorf("%q is empty", path.Join(rs.OplogName, files[i].Name))
				}
			}

			return nil
		})

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

// DeleteBackupFiles removes backup's artifacts from storage
func DeleteBackupFiles(meta *BackupMeta, stg storage.Storage) error {
	switch meta.Type {
	case defs.PhysicalBackup, defs.IncrementalBackup:
		return deletePhysicalBackupFiles(meta, stg)
	case defs.LogicalBackup:
		fallthrough
	default:
		var err error
		if version.IsLegacyArchive(meta.PBMVersion) {
			err = deleteLegacyLogicalBackupFiles(meta, stg)
		} else {
			err = deleteLogicalBackupFiles(meta, stg)
		}

		return err
	}
}

// DeleteBackupFiles removes backup's artifacts from storage
func deletePhysicalBackupFiles(meta *BackupMeta, stg storage.Storage) error {
	if version.HasFilelistFile(meta.PBMVersion) {
		for i := range meta.Replsets {
			rs := &meta.Replsets[i]

			filelistPath := path.Join(meta.Name, rs.Name, FilelistName)
			rdr, err := stg.SourceReader(filelistPath)
			if err != nil {
				if errors.Is(err, storage.ErrNotExist) {
					continue
				}

				return errors.Wrapf(err, "open %q", filelistPath)
			}
			defer rdr.Close()

			filelist, err := ReadFilelist(rdr)
			rdr.Close()
			if err != nil {
				return errors.Wrapf(err, "parse filelist")
			}

			rs.Files = filelist
		}
	}

	for _, r := range meta.Replsets {
		for _, f := range r.Files {
			fname := meta.Name + "/" + r.Name + "/" + f.Name + meta.Compression.Suffix()
			if f.Len != 0 {
				fname += fmt.Sprintf(".%d-%d", f.Off, f.Len)
			}
			err := stg.Delete(fname)
			if err != nil && !errors.Is(err, storage.ErrNotExist) {
				return errors.Wrapf(err, "delete %s", fname)
			}
		}
		for _, f := range r.Journal {
			fname := meta.Name + "/" + r.Name + "/" + f.Name + meta.Compression.Suffix()
			if f.Len != 0 {
				fname += fmt.Sprintf(".%d-%d", f.Off, f.Len)
			}
			err := stg.Delete(fname)
			if err != nil && !errors.Is(err, storage.ErrNotExist) {
				return errors.Wrapf(err, "delete %s", fname)
			}
		}
	}

	err := stg.Delete(meta.Name + defs.MetadataFileSuffix)
	if errors.Is(err, storage.ErrNotExist) {
		return nil
	}

	return errors.Wrap(err, "delete metadata file from storage")
}

// deleteLogicalBackupFiles removes backup's artifacts from storage
func deleteLogicalBackupFiles(meta *BackupMeta, stg storage.Storage) error {
	if stg.Type() == storage.Filesystem {
		return deleteLogicalBackupFilesFromFS(stg, meta.Name)
	}

	prefix := meta.Name + "/"
	files, err := stg.List(prefix, "")
	if err != nil {
		return errors.Wrapf(err, "get file list: %q", prefix)
	}

	eg := errgroup.Group{}
	for _, f := range files {
		ns := prefix + f.Name
		eg.Go(func() error {
			return errors.Wrapf(stg.Delete(ns), "delete %q", ns)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	bcpMF := meta.Name + defs.MetadataFileSuffix
	return errors.Wrapf(stg.Delete(bcpMF), "delete %q", bcpMF)
}

// deleteLogicalBackupFiles removes backup's artifacts from storage
func deleteLogicalBackupFilesFromFS(stg storage.Storage, bcpName string) error {
	if err := stg.Delete(bcpName); err != nil {
		return errors.Wrapf(err, "delete %q", bcpName)
	}

	bcpMetafile := bcpName + defs.MetadataFileSuffix
	return errors.Wrapf(stg.Delete(bcpMetafile), "delete %q", bcpMetafile)
}

// deleteLegacyLogicalBackupFiles removes backup's artifacts from storage
func deleteLegacyLogicalBackupFiles(meta *BackupMeta, stg storage.Storage) error {
	for _, r := range meta.Replsets {
		err := stg.Delete(r.OplogName)
		if err != nil && !errors.Is(err, storage.ErrNotExist) {
			return errors.Wrapf(err, "delete oplog %s", r.OplogName)
		}
		err = stg.Delete(r.DumpName)
		if err != nil && !errors.Is(err, storage.ErrNotExist) {
			return errors.Wrapf(err, "delete dump %s", r.DumpName)
		}
	}

	err := stg.Delete(meta.Name + defs.MetadataFileSuffix)
	if errors.Is(err, storage.ErrNotExist) {
		return nil
	}

	return errors.Wrap(err, "delete metadata file from storage")
}
