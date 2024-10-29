package blackhole

import (
	"context"
	"io"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

type Blackhole struct{}

var _ storage.Storage = &Blackhole{}

func New() *Blackhole {
	return &Blackhole{}
}

func (*Blackhole) Type() storage.Type {
	return storage.Blackhole
}

func (*Blackhole) Save(_ context.Context, _ string, data io.Reader, _ int64) error {
	_, err := io.Copy(io.Discard, data)
	return err
}

func (*Blackhole) List(_ context.Context, _, _ string) ([]storage.FileInfo, error) {
	return []storage.FileInfo{}, nil
}

func (*Blackhole) Delete(_ context.Context, _ string) error {
	return nil
}

func (*Blackhole) FileStat(_ context.Context, _ string) (storage.FileInfo, error) {
	return storage.FileInfo{}, nil
}

func (*Blackhole) Copy(_ context.Context, _, _ string) error {
	return nil
}

// nopReadCloser is a no operation ReadCloser
type nopReadCloser struct{}

func (nopReadCloser) Read(b []byte) (int, error) {
	return len(b), nil
}

func (nopReadCloser) Close() error {
	return nil
}

func (*Blackhole) SourceReader(context.Context, string) (io.ReadCloser, error) {
	return nopReadCloser{}, nil
}
