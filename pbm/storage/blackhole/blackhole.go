package blackhole

import (
	"io"
	"io/ioutil"
)

type Blackhole struct{}

func New() *Blackhole {
	return &Blackhole{}
}

func (*Blackhole) Save(_ string, data io.Reader, _ int) error {
	_, err := io.Copy(ioutil.Discard, data)
	return err
}

func (*Blackhole) Files(_ string) ([][]byte, error) { return [][]byte{}, nil }
func (*Blackhole) List(_ string) ([]string, error)  { return []string{}, nil }
func (*Blackhole) Delete(_ string) error            { return nil }
func (*Blackhole) CheckFile(_ string) error         { return nil }

// NopReadCloser is a no operation ReadCloser
type NopReadCloser struct{}

func (NopReadCloser) Read(b []byte) (int, error) {
	return len(b), nil
}
func (NopReadCloser) Close() error { return nil }

func (*Blackhole) SourceReader(name string) (io.ReadCloser, error) { return NopReadCloser{}, nil }
