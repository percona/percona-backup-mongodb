package backup

import (
	"context"
	"path"
	"runtime"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm/archive"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	sfs "github.com/percona/percona-backup-mongodb/pbm/storage/fs"
	"github.com/percona/percona-backup-mongodb/pbm/version"
)

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
func DeleteBackupFiles(stg storage.Storage, backupName string) error {
	if fs, ok := stg.(*sfs.FS); ok {
		return deleteBackupFromFS(fs, backupName)
	}

	files, err := stg.List(backupName, "")
	if err != nil {
		return errors.Wrap(err, "list files")
	}

	parallel := runtime.NumCPU()
	fileC := make(chan string, parallel)
	errC := make(chan error, parallel)

	wg := &sync.WaitGroup{}

	wg.Add(parallel)
	for range parallel {
		go func() {
			defer wg.Done()

			for f := range fileC {
				err := stg.Delete(backupName + "/" + f)
				if err != nil {
					errC <- errors.Wrapf(err, "delete %s", backupName+"/"+f)
				}
			}
		}()
	}

	go func() {
		for i := range files {
			fileC <- files[i].Name
		}
		close(fileC)

		wg.Wait()
		close(errC)
	}()

	var errs []error
	for err := range errC {
		errs = append(errs, err)
	}

	err = stg.Delete(backupName + defs.MetadataFileSuffix)
	if err != nil && !errors.Is(err, storage.ErrNotExist) {
		err = errors.Wrapf(err, "delete %s", backupName+defs.MetadataFileSuffix)
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func deleteBackupFromFS(stg *sfs.FS, backupName string) error {
	err1 := stg.Delete(backupName)
	if err1 != nil && !errors.Is(err1, storage.ErrNotExist) {
		err1 = errors.Wrapf(err1, "delete %s", backupName)
	}

	err2 := stg.Delete(backupName + defs.MetadataFileSuffix)
	if err2 != nil && !errors.Is(err2, storage.ErrNotExist) {
		err2 = errors.Wrapf(err2, "delete %s", backupName+defs.MetadataFileSuffix)
	}

	return errors.Join(err1, err2)
}
