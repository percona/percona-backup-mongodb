package storage

import (
	"io"
)

type Storage interface {
	Save(name string, data io.Reader) error
	SourceReader(name string) (io.ReadCloser, error)
	// CheckFile checks if file/object exists and isn't empty
	CheckFile(name string) error
	FilesList(suffix string) ([][]byte, error)
	Delete(name string) error
}
