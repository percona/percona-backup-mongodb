package storage

import (
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

)
type SpitMergeMiddleware struct {
	s Storage
}

func NewSplitMergeMW(s Storage) Storage {
	return &SpitMergeMiddleware{
		s: s,
	}
}

func (sm *SpitMergeMiddleware) Type() Type {
	return sm.s.Type()
}

func (sm *SpitMergeMiddleware) Save(name string, data io.Reader, options ...Option) error {
	return sm.s.Save(name, data, options...)
}

func (sm *SpitMergeMiddleware) SourceReader(name string) (io.ReadCloser, error) {
	return sm.s.SourceReader(name)
}

func (sm *SpitMergeMiddleware) FileStat(name string) (FileInfo, error) {
	return sm.s.FileStat(name)
}

func (sm *SpitMergeMiddleware) List(prefix, suffix string) ([]FileInfo, error) {
	return sm.s.List(prefix, suffix)
}
func (sm *SpitMergeMiddleware) Delete(name string) error {
	return sm.s.Delete(name)
}
func (sm *SpitMergeMiddleware) Copy(src, dst string) error {
	return sm.s.Copy(src, dst)
}

