package oci

import (
	"reflect"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

const (
	defaultUploadPartSize int64 = 10 * 1024 * 1024 // 10MiB
	defaultMaxUploadParts int32 = 10000
	defaultMaxObjSizeGB         = 5018 // 4.9 TB
)

//nolint:lll
type Config struct {
	Region      string      `bson:"region" json:"region" yaml:"region"`
	Namespace   string      `bson:"namespace,omitempty" json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Bucket      string      `bson:"bucket" json:"bucket" yaml:"bucket"`
	Prefix      string      `bson:"prefix,omitempty" json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Credentials Credentials `bson:"credentials" json:"credentials" yaml:"credentials"`

	UploadPartSize int64    `bson:"uploadPartSize,omitempty" json:"uploadPartSize,omitempty" yaml:"uploadPartSize,omitempty"`
	MaxUploadParts int32    `bson:"maxUploadParts,omitempty" json:"maxUploadParts,omitempty" yaml:"maxUploadParts,omitempty"`
	MaxObjSizeGB   *float64 `bson:"maxObjSizeGB,omitempty" json:"maxObjSizeGB,omitempty" yaml:"maxObjSizeGB,omitempty"`
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
	if cfg.MaxObjSizeGB != nil {
		v := *cfg.MaxObjSizeGB
		rv.MaxObjSizeGB = &v
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
	if cfg.MaxUploadParts <= 0 {
		cfg.MaxUploadParts = defaultMaxUploadParts
	}

	return nil
}

func (cfg *Config) GetMaxObjSizeGB() float64 {
	if cfg.MaxObjSizeGB != nil && *cfg.MaxObjSizeGB >= storage.MinValidMaxObjSizeGB {
		return *cfg.MaxObjSizeGB
	}
	return defaultMaxObjSizeGB
}
