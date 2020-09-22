package storage

import (
	"errors"
	"io"
)

// ErrNotExist is an error for file isn't exists on storage
var ErrNotExist = errors.New("no such file")

type Storage interface {
	Save(name string, data io.Reader, size int) error
	SourceReader(name string) (io.ReadCloser, error)
	// CheckFile checks if file/object exists and isn't empty
	CheckFile(name string) error
	List(prefix string) ([]string, error)
	Files(suffix string) ([][]byte, error)
	// Delete deletes given file.
	// It returns storage.ErrNotExist if a file isn't exists
	Delete(name string) error
}
