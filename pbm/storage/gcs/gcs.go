package gcs

import (
	"context"
	"fmt"
	"io"
	"path"
	"strings"

	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

type Config struct {
	Bucket      string      `bson:"bucket" json:"bucket" yaml:"bucket"`
	Prefix      string      `bson:"prefix" json:"prefix" yaml:"prefix"`
	Credentials Credentials `bson:"credentials" json:"credentials" yaml:"credentials"`

	// The maximum number of bytes that the Writer will attempt to send in a single request.
	// https://pkg.go.dev/cloud.google.com/go/storage#Writer
	ChunkSize *int `bson:"chunkSize,omitempty" json:"chunkSize,omitempty" yaml:"chunkSize,omitempty"`
}

func (cfg *Config) Clone() *Config {
	if cfg == nil {
		return nil
	}

	rv := *cfg
	return &rv
}

type Credentials struct {
	ProjectId  string `bson:"projectId" json:"projectId,omitempty" yaml:"projectId,omitempty"`
	PrivateKey string `bson:"privateKey" json:"privateKey,omitempty" yaml:"privateKey,omitempty"`
}

type GCS struct {
	opts         *Config
	bucketHandle *gcs.BucketHandle
	log          log.LogEvent
}

func New(opts *Config, node string, l log.LogEvent) (*GCS, error) {
	ctx := context.Background()
	var client *gcs.Client
	var err error

	if opts.Credentials.ProjectId != "" && opts.Credentials.PrivateKey != "" {
		credStr := fmt.Sprintf(`{
			"type": "service_account",
          	"project_id": "%s",
			"private_key": "%s",
			"client_email": "service@%s.iam.gserviceaccount.com",
			"auth_uri": "https://accounts.google.com/o/oauth2/auth",
			"token_uri": "https://oauth2.googleapis.com/token",
			"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
			"client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/%s.iam.gserviceaccount.com",
			"universe_domain": "googleapis.com"
		}`,
			opts.Credentials.ProjectId,
			opts.Credentials.PrivateKey,
			opts.Credentials.ProjectId,
			opts.Credentials.ProjectId,
		)

		credentials := option.WithCredentialsJSON([]byte(credStr))

		client, err = gcs.NewClient(ctx, credentials)
	} else {
		client, err = gcs.NewClient(ctx)
	}

	if err != nil {
		return nil, errors.Wrap(err, "create GCS client")
	}

	return &GCS{
		opts:         opts,
		bucketHandle: client.Bucket(opts.Bucket),
		log:          l,
	}, nil
}

func (*GCS) Type() storage.Type {
	return storage.GCS
}

func (g *GCS) Save(name string, data io.Reader, size int64) error {
	ctx := context.Background()

	w := g.bucketHandle.Object(path.Join(g.opts.Prefix, name)).NewWriter(ctx)

	if g.opts.ChunkSize != nil {
		w.ChunkSize = *g.opts.ChunkSize
	}

	if _, err := io.Copy(w, data); err != nil {
		return errors.Wrap(err, "save data")
	}

	if err := w.Close(); err != nil {
		return errors.Wrap(err, "writer close")
	}

	return nil
}

func (g *GCS) SourceReader(name string) (io.ReadCloser, error) {
	ctx := context.Background()

	reader, err := g.bucketHandle.Object(path.Join(g.opts.Prefix, name)).NewReader(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "object not found")
	}

	return reader, nil

}

func (g *GCS) FileStat(name string) (storage.FileInfo, error) {
	ctx := context.Background()

	attrs, err := g.bucketHandle.Object(path.Join(g.opts.Prefix, name)).Attrs(ctx)
	if err != nil {
		if errors.Is(err, gcs.ErrObjectNotExist) {
			return storage.FileInfo{}, storage.ErrNotExist
		}

		return storage.FileInfo{}, errors.Wrap(err, "get properties")
	}

	inf := storage.FileInfo{
		Name: attrs.Name,
		Size: attrs.Size,
	}

	if inf.Size == 0 {
		return inf, storage.ErrEmpty
	}

	return inf, nil
}

func (g *GCS) List(prefix, suffix string) ([]storage.FileInfo, error) {
	ctx := context.Background()

	prfx := path.Join(g.opts.Prefix, prefix)

	if prfx != "" && !strings.HasSuffix(prfx, "/") {
		prfx += "/"
	}

	query := &gcs.Query{
		Prefix: prfx,
	}

	var files []storage.FileInfo
	it := g.bucketHandle.Objects(ctx, query)
	for {
		attrs, err := it.Next()

		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, errors.Wrap(err, "list objects")
		}

		name := attrs.Name
		name = strings.TrimPrefix(name, prfx)
		if len(name) == 0 {
			continue
		}
		if name[0] == '/' {
			name = name[1:]
		}

		if suffix != "" && !strings.HasSuffix(name, suffix) {
			continue
		}

		files = append(files, storage.FileInfo{
			Name: name,
			Size: attrs.Size,
		})
	}

	return files, nil
}

func (g *GCS) Delete(name string) error {
	ctx := context.Background()

	err := g.bucketHandle.Object(path.Join(g.opts.Prefix, name)).Delete(ctx)
	if err != nil {
		if errors.Is(err, gcs.ErrObjectNotExist) {
			return storage.ErrNotExist
		}
		return errors.Wrap(err, "delete object")
	}

	return nil
}

func (g *GCS) Copy(src, dst string) error {
	ctx := context.Background()

	srcObj := g.bucketHandle.Object(path.Join(g.opts.Prefix, src))
	dstObj := g.bucketHandle.Object(path.Join(g.opts.Prefix, dst))

	_, err := dstObj.CopierFrom(srcObj).Run(ctx)
	return err
}
