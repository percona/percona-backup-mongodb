package webdav

import (
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/pkg/errors"
	"github.com/studio-b12/gowebdav"
	"io"
	"os"
	"path"
)

type Conf struct {
	ServerURL   string      `bson:"serverUrl,omitempty" json:"endpointUrl" yaml:"endpointUrl,omitempty"`
	Prefix      string      `bson:"prefix,omitempty" json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Credentials Credentials `bson:"credentials" json:"-" yaml:"credentials"`
}

type Credentials struct {
	Username string `bson:"username" json:"username,omitempty" yaml:"username,omitempty"`
	Password string `bson:"password" json:"password,omitempty" yaml:"password,omitempty"`
}

type WebDAV struct {
	opts   Conf
	log    *log.Event
	client *gowebdav.Client
}

func New(opts Conf, l *log.Event) (*WebDAV, error) {
	w := &WebDAV{
		opts: opts,
		log:  l,
	}

	w.client = gowebdav.NewClient(opts.ServerURL, opts.Credentials.Username, opts.Credentials.Password)
	err := w.client.Connect()
	if err != nil {
		return nil, errors.Wrap(err, "connect to WebDAV server")
	}

	return w, nil
}

func (*WebDAV) Type() storage.Type {
	return storage.WebDAV
}

func (w *WebDAV) Save(name string, data io.Reader, sizeb int64) error {
	err := w.client.WriteStream(path.Join(w.opts.Prefix, name), data, 0644)
	if err != nil {
		return errors.Wrap(err, "upload to WebDAV server")
	}
	return nil
}

func (w *WebDAV) List(prefix, suffix string) ([]storage.FileInfo, error) {
	var sfiles []storage.FileInfo

	wfiles, err := w.client.ReadDir(path.Join(w.opts.Prefix, prefix))
	if err != nil {
		return nil, errors.Wrap(err, "list files on WebDAV server")
	}

	for _, wfile := range wfiles {
		sfiles = append(sfiles, storage.FileInfo{
			Name: wfile.Name(),
			Size: wfile.Size(),
		})
	}

	return sfiles, nil
}

func (w *WebDAV) Copy(src, dst string) error {
	err := w.client.Copy(path.Join(w.opts.Prefix, path.Join(w.opts.Prefix, src)), dst, true)
	if err != nil {
		return errors.Wrap(err, "copy file on WebDAV server")
	}
	return err
}

func (w *WebDAV) FileStat(name string) (inf storage.FileInfo, err error) {
	stat, err := w.client.Stat(path.Join(w.opts.Prefix, name))
	if err != nil {
		if perr, ok := err.(*os.PathError); ok && perr.Err.(gowebdav.StatusError).Status == 404 {
			return inf, storage.ErrNotExist
		}

		return inf, errors.Wrap(err, "get file info from WebDAV server")
	}

	inf.Name = name
	inf.Size = stat.Size()

	if inf.Size == 0 {
		return inf, storage.ErrEmpty
	}

	return inf, nil
}

func (w *WebDAV) SourceReader(name string) (io.ReadCloser, error) {
	return w.client.ReadStream(path.Join(w.opts.Prefix, name))
}

func (w *WebDAV) Delete(name string) error {
	err := w.client.Remove(path.Join(w.opts.Prefix, name))

	// Warning: GoWebDAV library doesn't return an error when the resource to be deleted doesn't exist!
	if err != nil {
		return errors.Wrap(err, "delete file from WebDAV server")
	}

	return nil
}
