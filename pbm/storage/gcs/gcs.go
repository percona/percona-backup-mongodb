package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
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

	Retryer *Retryer `bson:"retryer,omitempty" json:"retryer,omitempty" yaml:"retryer,omitempty"`
}

type Credentials struct {
	ProjectID  string `bson:"projectId" json:"projectId,omitempty" yaml:"projectId,omitempty"`
	PrivateKey string `bson:"privateKey" json:"privateKey,omitempty" yaml:"privateKey,omitempty"`
}

type Retryer struct {
	// BackoffInitial is the initial value of the retry period.
	// https://pkg.go.dev/github.com/googleapis/gax-go/v2@v2.12.3#Backoff.Initial
	BackoffInitial time.Duration `bson:"backoffInitial" json:"backoffInitial" yaml:"backoffInitial"`

	// BackoffMax is the maximum value of the retry period.
	// https://pkg.go.dev/github.com/googleapis/gax-go/v2@v2.12.3#Backoff.Max
	BackoffMax time.Duration `bson:"backoffMax" json:"backoffMax" yaml:"backoffMax"`
}

type ServiceAccountCredentials struct {
	Type                string `json:"type"`
	ProjectID           string `json:"project_id"`
	PrivateKey          string `json:"private_key"`
	ClientEmail         string `json:"client_email"`
	AuthUri             string `json:"auth_uri"`
	TokenUri            string `json:"token_uri"`
	UniverseDomain      string `json:"universe_domain"`
	AuthProviderCertUrl string `json:"auth_provider_x509_cert_url"`
	ClientCertUrl       string `json:"client_x509_cert_url"`
}

type GCS struct {
	opts         *Config
	bucketHandle *gcs.BucketHandle
	log          log.LogEvent
}

func (cfg *Config) Clone() *Config {
	if cfg == nil {
		return nil
	}

	rv := *cfg
	return &rv
}

func New(opts *Config, node string, l log.LogEvent) (*GCS, error) {
	ctx := context.Background()
	var client *gcs.Client
	var err error

	if opts.Credentials.ProjectID != "" && opts.Credentials.PrivateKey != "" {
		creds := ServiceAccountCredentials{
			Type:                "service_account",
			ProjectID:           opts.Credentials.ProjectID,
			PrivateKey:          opts.Credentials.PrivateKey,
			ClientEmail:         fmt.Sprintf("service@%s.iam.gserviceaccount.com", opts.Credentials.ProjectID),
			AuthUri:             "https://accounts.google.com/o/oauth2/auth",
			TokenUri:            "https://oauth2.googleapis.com/token",
			UniverseDomain:      "googleapis.com",
			AuthProviderCertUrl: "https://www.googleapis.com/oauth2/v1/certs",
			ClientCertUrl: fmt.Sprintf(
				"https://www.googleapis.com/robot/v1/metadata/x509/%s.iam.gserviceaccount.com",
				opts.Credentials.ProjectID,
			),
		}

		credsJson, err := json.Marshal(creds)
		if err != nil {
			return nil, errors.Wrap(err, "marshal GCS credentials")
		}

		credentials := option.WithCredentialsJSON(credsJson)

		client, err = gcs.NewClient(ctx, credentials)
	} else {
		client, err = gcs.NewClient(ctx)
	}

	if err != nil {
		return nil, errors.Wrap(err, "create GCS client")
	}

	bucketHandle := client.Bucket(opts.Bucket)

	if opts.Retryer != nil {
		bucketHandle = bucketHandle.Retryer(
			gcs.WithBackoff(gax.Backoff{
				Initial: opts.Retryer.BackoffInitial,
				Max:     opts.Retryer.BackoffMax,
			}),

			gcs.WithPolicy(gcs.RetryAlways),
		)
	}

	return &GCS{
		opts:         opts,
		bucketHandle: bucketHandle,
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
