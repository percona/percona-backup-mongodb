package storage

import (
	"errors"
	"io"
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
