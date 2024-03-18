package storage

import (
	"context"
	"io"

	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

var (
	// ErrNotExist is an error for file doesn't exists on storage
	ErrNotExist = errors.New("no such file")
	ErrEmpty    = errors.New("file is empty")
)

// Type represents a type of the destination storage for backups
type Type string

const (
	Undef      Type = ""
	S3         Type = "s3"
	Azure      Type = "azure"
	Filesystem Type = "filesystem"
	BlackHole  Type = "blackhole"
)

type FileInfo struct {
	Name string // with path
	Size int64
}

type Storage interface {
	Type() Type
	Save(name string, data io.Reader, size int64) error
	SourceReader(name string) (io.ReadCloser, error)
	// FileStat returns file info. It returns error if file is empty or not exists.
	FileStat(name string) (FileInfo, error)
	// List scans path with prefix and returns all files with given suffix.
	// Both prefix and suffix can be omitted.
	List(prefix, suffix string) ([]FileInfo, error)
	// Delete deletes given file.
	// It returns storage.ErrNotExist if a file doesn't exists.
	Delete(name string) error
	// Copy makes a copy of the src objec/file under dst name
	Copy(src, dst string) error
}

// ParseType parses string and returns storage type
func ParseType(s string) Type {
	switch s {
	case string(S3):
		return S3
	case string(Azure):
		return Azure
	case string(Filesystem):
		return Filesystem
	case string(BlackHole):
		return BlackHole
	default:
		return Undef
	}
}

// HasReadAccess checks if the storage has read access to the specified file.
// It returns true if read access is available, otherwise it returns false.
// If an error occurs during the check, it returns the error.
func HasReadAccess(ctx context.Context, stg Storage) (bool, error) {
	_, err := stg.FileStat(defs.StorInitFile)
	return err == nil, err
}

// rwError multierror for the read/compress/write-to-store operations set
type rwError struct {
	read     error
	compress error
	write    error
}

func (rwe rwError) Error() string {
	var r string
	if rwe.read != nil {
		r += "read data: " + rwe.read.Error() + "."
	}
	if rwe.compress != nil {
		r += "compress data: " + rwe.compress.Error() + "."
	}
	if rwe.write != nil {
		r += "write data: " + rwe.write.Error() + "."
	}

	return r
}

func (rwe rwError) nil() bool {
	return rwe.read == nil && rwe.compress == nil && rwe.write == nil
}

type Source interface {
	io.WriterTo
}

type Canceller interface {
	Cancel()
}

// ErrCancelled means backup was canceled
var ErrCancelled = errors.New("backup canceled")

// Upload writes data to dst from given src and returns an amount of written bytes
func Upload(
	ctx context.Context,
	src Source,
	dst Storage,
	compression compress.CompressionType,
	compressLevel *int,
	fname string,
	sizeb int64,
) (int64, error) {
	r, pw := io.Pipe()

	w, err := compress.Compress(pw, compression, compressLevel)
	if err != nil {
		return 0, err
	}

	var rwErr rwError
	var n int64
	go func() {
		n, rwErr.read = src.WriteTo(w)
		rwErr.compress = w.Close()
		pw.Close()
	}()

	saveDone := make(chan struct{})
	go func() {
		rwErr.write = dst.Save(fname, r, sizeb)
		saveDone <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		if c, ok := src.(Canceller); ok {
			c.Cancel()
		}

		err := r.Close()
		if err != nil {
			return 0, errors.Wrap(err, "cancel backup: close reader")
		}
		return 0, ErrCancelled
	case <-saveDone:
	}

	r.Close()

	if !rwErr.nil() {
		return 0, rwErr
	}

	return n, nil
}
