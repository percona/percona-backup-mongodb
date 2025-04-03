package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"reflect"
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
	ClientEmail string `bson:"clientEmail" json:"clientEmail,omitempty" yaml:"clientEmail,omitempty"`
	PrivateKey  string `bson:"privateKey" json:"privateKey,omitempty" yaml:"privateKey,omitempty"`
}

type Retryer struct {
	// BackoffInitial is the initial value of the retry period.
	// https://pkg.go.dev/github.com/googleapis/gax-go/v2@v2.12.3#Backoff.Initial
	BackoffInitial time.Duration `bson:"backoffInitial" json:"backoffInitial" yaml:"backoffInitial"`

	// BackoffMax is the maximum value of the retry period.
	// https://pkg.go.dev/github.com/googleapis/gax-go/v2@v2.12.3#Backoff.Max
	BackoffMax time.Duration `bson:"backoffMax" json:"backoffMax" yaml:"backoffMax"`

	// BackoffMultiplier is the factor by which the retry period increases.
	// https://pkg.go.dev/github.com/googleapis/gax-go/v2@v2.12.3#Backoff.Multiplier
	BackoffMultiplier float64 `bson:"backoffMultiplier" json:"backoffMultiplier" yaml:"backoffMultiplier"`
}

type ServiceAccountCredentials struct {
	Type                string `json:"type"`
	PrivateKey          string `json:"private_key"`
	ClientEmail         string `json:"client_email"`
	AuthURI             string `json:"auth_uri"`
	TokenURI            string `json:"token_uri"`
	UniverseDomain      string `json:"universe_domain"`
	AuthProviderCertURL string `json:"auth_provider_x509_cert_url"`
	ClientCertURL       string `json:"client_x509_cert_url"`
}

type GCS struct {
	opts         *Config
	bucketHandle *gcs.BucketHandle
	log          log.LogEvent

	d *Download
}

func (cfg *Config) Clone() *Config {
	if cfg == nil {
		return nil
	}

	rv := *cfg
	return &rv
}

func (cfg *Config) Equal(other *Config) bool {
	if cfg == nil || other == nil {
		return cfg == other
	}

	if cfg.Bucket != other.Bucket {
		return false
	}
	if cfg.Prefix != other.Prefix {
		return false
	}
	if cfg.ChunkSize != other.ChunkSize {
		return false
	}

	if !reflect.DeepEqual(cfg.Credentials, other.Credentials) {
		return false
	}

	return true
}

func New(opts *Config, node string, l log.LogEvent) (*GCS, error) {
	g := &GCS{
		opts: opts,
		log:  l,
	}

	cli, err := g.gcsClient()
	if err != nil {
		return nil, errors.Wrap(err, "GCS client")
	}

	bucketHandle := cli.Bucket(opts.Bucket)

	if opts.Retryer != nil {
		bucketHandle = bucketHandle.Retryer(
			gcs.WithBackoff(gax.Backoff{
				Initial:    opts.Retryer.BackoffInitial,
				Max:        opts.Retryer.BackoffMax,
				Multiplier: opts.Retryer.BackoffMultiplier,
			}),

			gcs.WithPolicy(gcs.RetryAlways),
		)
	}

	g.bucketHandle = bucketHandle

	g.d = &Download{
		gcs:      g,
		arenas:   []*storage.Arena{storage.NewArena(storage.DownloadChuckSizeDefault, storage.DownloadChuckSizeDefault)},
		spanSize: storage.DownloadChuckSizeDefault,
		cc:       1,
	}

	return g, nil
}

func (*GCS) Type() storage.Type {
	return storage.GCS
}

func (g *GCS) Save(name string, data io.Reader, _ ...storage.Option) error {
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

func (g *GCS) gcsClient() (*gcs.Client, error) {
	ctx := context.Background()

	if g.opts.Credentials.PrivateKey == "" || g.opts.Credentials.ClientEmail == "" {
		return nil, errors.New("clientEmail and privateKey are required for GCS credentials")
	}

	creds, err := json.Marshal(ServiceAccountCredentials{
		Type:                "service_account",
		PrivateKey:          g.opts.Credentials.PrivateKey,
		ClientEmail:         g.opts.Credentials.ClientEmail,
		AuthURI:             "https://accounts.google.com/o/oauth2/auth",
		TokenURI:            "https://oauth2.googleapis.com/token",
		UniverseDomain:      "googleapis.com",
		AuthProviderCertURL: "https://www.googleapis.com/oauth2/v1/certs",
		ClientCertURL: fmt.Sprintf(
			"https://www.googleapis.com/robot/v1/metadata/x509/%s",
			g.opts.Credentials.ClientEmail,
		),
	})
	if err != nil {
		return nil, errors.Wrap(err, "marshal GCS credentials")
	}

	return gcs.NewClient(ctx, option.WithCredentialsJSON(creds))
}
