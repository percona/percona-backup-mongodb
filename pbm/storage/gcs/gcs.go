package gcs

import (
	"io"
	"path"
	"reflect"
	"strings"
	"time"

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
	ChunkSize int `bson:"chunkSize,omitempty" json:"chunkSize,omitempty" yaml:"chunkSize,omitempty"`

	Retryer *Retryer `bson:"retryer,omitempty" json:"retryer,omitempty" yaml:"retryer,omitempty"`
}

type Credentials struct {
	// JSON credentials (service account)
	ClientEmail string `bson:"clientEmail" json:"clientEmail,omitempty" yaml:"clientEmail,omitempty"`
	PrivateKey  string `bson:"privateKey" json:"privateKey,omitempty" yaml:"privateKey,omitempty"`

	// HMAC credentials for XML API (S3 compatibility)
	HMACAccessKey string `bson:"hmacAccessKey" json:"hmacAccessKey,omitempty" yaml:"hmacAccessKey,omitempty"`
	HMACSecret    string `bson:"hmacSecret" json:"hmacSecret,omitempty" yaml:"hmacSecret,omitempty"`
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
	// Ignored for MinIO (only Initial and Max used)
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

type gcsClient interface {
	save(name string, data io.Reader, options ...storage.Option) error
	fileStat(name string) (storage.FileInfo, error)
	list(prefix, suffix string) ([]storage.FileInfo, error)
	delete(name string) error
	copy(src, dst string) error
	getPartialObject(name string, buf *storage.Arena, start, length int64) (io.ReadCloser, error)
}

type GCS struct {
	opts *Config
	log  log.LogEvent

	client gcsClient
	d      *Download
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

// IsSameStorage identifies the same instance of the GCS storage.
func (cfg *Config) IsSameStorage(other *Config) bool {
	if cfg == nil || other == nil {
		return cfg == other
	}

	if cfg.Bucket != other.Bucket {
		return false
	}
	if cfg.Prefix != other.Prefix {
		return false
	}

	return true
}

func New(opts *Config, node string, l log.LogEvent) (*GCS, error) {
	g := &GCS{
		opts: opts,
		log:  l,
	}

	if g.opts.Credentials.HMACAccessKey != "" && g.opts.Credentials.HMACSecret != "" {
		hc, err := newHMACClient(g.opts, g.log)
		if err != nil {
			return nil, errors.Wrap(err, "new hmac client")
		}
		g.client = hc
	} else {
		gc, err := newGoogleClient(g.opts, g.log)
		if err != nil {
			return nil, errors.Wrap(err, "new google client")
		}
		g.client = gc
	}

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

func (g *GCS) Save(name string, data io.Reader, options ...storage.Option) error {
	return g.client.save(name, data, options...)
}

func (g *GCS) FileStat(name string) (storage.FileInfo, error) {
	return g.client.fileStat(name)
}

func (g *GCS) List(prefix, suffix string) ([]storage.FileInfo, error) {
	prfx := path.Join(g.opts.Prefix, prefix)

	if prfx != "" && !strings.HasSuffix(prfx, "/") {
		prfx += "/"
	}

	return g.client.list(prfx, suffix)
}

func (g *GCS) Delete(name string) error {
	return g.client.delete(name)
}

func (g *GCS) Copy(src, dst string) error {
	return g.client.copy(src, dst)
}
