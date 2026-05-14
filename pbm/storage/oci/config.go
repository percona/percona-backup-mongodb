package oci

import (
	"reflect"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

const (
	// OCI Object Storage limits: https://docs.oracle.com/en-us/iaas/Content/Object/Concepts/objectstorageoverview.htm
	maxUploadPartSize int64 = 50 * 1024 * 1024 * 1024 // 50 GiB
	maxUploadParts    int32 = 10000
	maxObjSizeGB            = 10 * 1024 // 10 TiB

	// Keep PBM's split/merge threshold slightly below OCI's object size limit.
	defaultMaxObjSizeGB = 10138 // 9.9 TiB application limit

	defaultUploadPartSize int64 = 10 * 1024 * 1024 // 10 MiB
	// OCI CLI documents the multipart lower bound as greater than 10 MiB,
	// while OCI SDK stream uploads default to 10 MiB.
	minUploadPartSize = defaultUploadPartSize

	// Match OCI SDK's defaultNumberOfGoroutines for UploadManager.
	defaultUploadConcurrency = 5
	// Match OCI CLI's --parallel-upload-count maximum instead of the SDK's much higher bound.
	maxUploadConcurrency = 1000

	defaultRetryMaxAttempts = 8
	defaultRetryMaxBackoff  = 30 * time.Second
	retryBackoffBase        = 2.0
)

//nolint:lll
type Config struct {
	Region      string      `bson:"region" json:"region" yaml:"region"`
	Namespace   string      `bson:"namespace,omitempty" json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Bucket      string      `bson:"bucket" json:"bucket" yaml:"bucket"`
	Prefix      string      `bson:"prefix,omitempty" json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Credentials Credentials `bson:"credentials" json:"credentials" yaml:"credentials"`
	Retryer     *Retryer    `bson:"retryer,omitempty" json:"retryer,omitempty" yaml:"retryer,omitempty"`

	UploadPartSize int64 `bson:"uploadPartSize,omitempty" json:"uploadPartSize,omitempty" yaml:"uploadPartSize,omitempty"`
	// Increasing upload concurrency is not recommended by the OCI SDK because it can cause
	// 409 responses or client timeouts.
	UploadConcurrency int `bson:"uploadConcurrency,omitempty" json:"uploadConcurrency,omitempty" yaml:"uploadConcurrency,omitempty"`
}

// Retryer configures OCI SDK retries for Object Storage requests.
type Retryer struct {
	// MaxAttempts is the total number of attempts, including the first call.
	// 0 means use the PBM default; 1 disables retries. Unlimited retries are not supported.
	MaxAttempts int `bson:"maxAttempts" json:"maxAttempts" yaml:"maxAttempts"`
	// MaxBackoff caps the exponential retry backoff. 0 means use the PBM default.
	MaxBackoff time.Duration `bson:"maxBackoff" json:"maxBackoff" yaml:"maxBackoff"`
}

//nolint:lll
type Credentials struct {
	Tenancy              storage.MaskedString `bson:"tenancy" json:"tenancy,omitempty" yaml:"tenancy,omitempty"`
	User                 storage.MaskedString `bson:"user" json:"user,omitempty" yaml:"user,omitempty"`
	Fingerprint          storage.MaskedString `bson:"fingerprint" json:"fingerprint,omitempty" yaml:"fingerprint,omitempty"`
	PrivateKey           storage.MaskedString `bson:"privateKey" json:"privateKey,omitempty" yaml:"privateKey,omitempty"`
	PrivateKeyPassphrase storage.MaskedString `bson:"privateKeyPassphrase,omitempty" json:"privateKeyPassphrase,omitempty" yaml:"privateKeyPassphrase,omitempty"`
}

func (cfg *Config) Clone() *Config {
	if cfg == nil {
		return nil
	}

	rv := *cfg
	if cfg.Retryer != nil {
		v := *cfg.Retryer
		rv.Retryer = &v
	}

	return &rv
}

func (cfg *Config) Equal(other *Config) bool {
	return reflect.DeepEqual(cfg, other)
}

// IsSameStorage identifies the same instance of the OCI storage.
func (cfg *Config) IsSameStorage(other *Config) bool {
	if cfg == nil || other == nil {
		return cfg == other
	}

	if cfg.Region != other.Region {
		return false
	}
	if cfg.Namespace != other.Namespace {
		return false
	}
	if cfg.Bucket != other.Bucket {
		return false
	}
	if cfg.Prefix != other.Prefix {
		return false
	}

	return true
}

func (cfg *Config) Cast() error {
	if cfg == nil {
		return errors.New("missing oci configuration with oci storage type")
	}
	if cfg.UploadPartSize <= 0 {
		cfg.UploadPartSize = defaultUploadPartSize
	}
	if cfg.UploadPartSize > maxUploadPartSize {
		return errors.Errorf("uploadPartSize cannot exceed %d", maxUploadPartSize)
	}
	if cfg.UploadConcurrency <= 0 {
		cfg.UploadConcurrency = defaultUploadConcurrency
	}
	if cfg.UploadConcurrency > maxUploadConcurrency {
		return errors.Errorf("uploadConcurrency cannot exceed %d", maxUploadConcurrency)
	}
	if cfg.Retryer == nil {
		r := retryerWithDefaults(nil)
		cfg.Retryer = &r
	} else {
		if cfg.Retryer.MaxAttempts < 0 {
			return errors.New("retryer.maxAttempts cannot be negative")
		}
		if cfg.Retryer.MaxBackoff < 0 {
			return errors.New("retryer.maxBackoff cannot be negative")
		}
		r := retryerWithDefaults(cfg.Retryer)
		cfg.Retryer = &r
	}

	return nil
}

func retryerWithDefaults(cfg *Retryer) Retryer {
	if cfg == nil {
		return Retryer{
			MaxAttempts: defaultRetryMaxAttempts,
			MaxBackoff:  defaultRetryMaxBackoff,
		}
	}

	r := *cfg
	if r.MaxAttempts == 0 {
		r.MaxAttempts = defaultRetryMaxAttempts
	}
	if r.MaxBackoff == 0 {
		r.MaxBackoff = defaultRetryMaxBackoff
	}

	return r
}
