package oss

import (
	"errors"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

const (
	defaultOSSRegion       = "us-east-1"
	maxPart          int32 = 10000

	defaultRetryMaxAttempts       = 5
	defaultRetryBaseDelay         = 30 * time.Millisecond
	defaultRetryerMaxBackoff      = 300 * time.Second
	defaultSessionDurationSeconds = 3600
	defaultConnectTimeout         = 5 * time.Second
	defaultMaxObjSizeGB           = 48700 // 48.8 TB
)

//nolint:lll
type Config struct {
	Region      string `bson:"region" json:"region" yaml:"region"`
	EndpointURL string `bson:"endpointUrl,omitempty" json:"endpointUrl" yaml:"endpointUrl,omitempty"`

	Bucket      string      `bson:"bucket" json:"bucket" yaml:"bucket"`
	Prefix      string      `bson:"prefix,omitempty" json:"prefix,omitempty" yaml:"prefix,omitempty"`
	Credentials Credentials `bson:"credentials" json:"credentials" yaml:"credentials"`

	Retryer *Retryer `bson:"retryer,omitempty" json:"retryer,omitempty" yaml:"retryer,omitempty"`

	ConnectTimeout time.Duration `bson:"connectTimeout" json:"connectTimeout" yaml:"connectTimeout"`
	UploadPartSize int64         `bson:"uploadPartSize,omitempty" json:"uploadPartSize,omitempty" yaml:"uploadPartSize,omitempty"`
	MaxUploadParts int32         `bson:"maxUploadParts,omitempty" json:"maxUploadParts,omitempty" yaml:"maxUploadParts,omitempty"`
	MaxObjSizeGB   *float64      `bson:"maxObjSizeGB,omitempty" json:"maxObjSizeGB,omitempty" yaml:"maxObjSizeGB,omitempty"`

	ServerSideEncryption *SSE `bson:"serverSideEncryption,omitempty" json:"serverSideEncryption,omitempty" yaml:"serverSideEncryption,omitempty"`
}

//nolint:lll
type SSE struct {
	EncryptionMethod    string               `bson:"encryptionMethod,omitempty" json:"encryptionMethod,omitempty" yaml:"encryptionMethod,omitempty"`
	EncryptionAlgorithm string               `bson:"encryptionAlgorithm,omitempty" json:"encryptionAlgorithm,omitempty" yaml:"encryptionAlgorithm,omitempty"`
	EncryptionKeyID     storage.MaskedString `bson:"encryptionKeyId,omitempty" json:"encryptionKeyId,omitempty" yaml:"encryptionKeyId,omitempty"`
}

type Retryer struct {
	MaxAttempts int           `bson:"maxAttempts" json:"maxAttempts" yaml:"maxAttempts"`
	MaxBackoff  time.Duration `bson:"maxBackoff" json:"maxBackoff" yaml:"maxBackoff"`
	BaseDelay   time.Duration `bson:"baseDelay" json:"baseDelay" yaml:"baseDelay"`
}

//nolint:lll
type Credentials struct {
	AccessKeyID     storage.MaskedString `bson:"accessKeyId" json:"accessKeyId,omitempty" yaml:"accessKeyId,omitempty"`
	AccessKeySecret storage.MaskedString `bson:"accessKeySecret" json:"accessKeySecret,omitempty" yaml:"accessKeySecret,omitempty"`
	SecurityToken   storage.MaskedString `bson:"securityToken" json:"securityToken,omitempty" yaml:"securityToken,omitempty"`
	RoleARN         storage.MaskedString `bson:"roleArn,omitempty" json:"roleArn,omitempty" yaml:"roleArn,omitempty"`
	SessionName     storage.MaskedString `bson:"sessionName,omitempty" json:"sessionName,omitempty" yaml:"sessionName,omitempty"`
}

// IsSameStorage identifies the same instance of the OSS storage.
func (cfg *Config) IsSameStorage(other *Config) bool {
	if cfg == nil || other == nil {
		return cfg == other
	}

	if cfg.Region != other.Region {
		return false
	}
	if cfg.Bucket != other.Bucket {
		return false
	}
	if cfg.Prefix != other.Prefix {
		return false
	}
	if cfg.EndpointURL != other.EndpointURL {
		return false
	}

	return true
}

func (cfg *Config) Cast() error {
	if cfg == nil {
		return errors.New("missing oss configuration with oss storage type")
	}
	if cfg.Region == "" {
		cfg.Region = defaultOSSRegion
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = defaultConnectTimeout
	}
	if cfg.Retryer != nil {
		if cfg.Retryer.MaxAttempts == 0 {
			cfg.Retryer.MaxAttempts = defaultRetryMaxAttempts
		}
		if cfg.Retryer.BaseDelay == 0 {
			cfg.Retryer.BaseDelay = defaultRetryBaseDelay
		}
		if cfg.Retryer.MaxBackoff == 0 {
			cfg.Retryer.MaxBackoff = defaultRetryerMaxBackoff
		}
	} else {
		cfg.Retryer = &Retryer{
			MaxAttempts: defaultRetryMaxAttempts,
			MaxBackoff:  defaultRetryerMaxBackoff,
			BaseDelay:   defaultRetryBaseDelay,
		}
	}
	if cfg.MaxUploadParts <= 0 {
		cfg.MaxUploadParts = maxPart
	}
	if cfg.UploadPartSize <= 0 {
		cfg.UploadPartSize = oss.DefaultUploadPartSize
	}
	return nil
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
	if cfg.Retryer != nil {
		v := *cfg.Retryer
		rv.Retryer = &v
	}
	if cfg.ServerSideEncryption != nil {
		a := *cfg.ServerSideEncryption
		rv.ServerSideEncryption = &a
	}
	return &rv
}

func (cfg *Config) GetMaxObjSizeGB() float64 {
	if cfg.MaxObjSizeGB != nil && *cfg.MaxObjSizeGB >= storage.MinValidMaxObjSizeGB {
		return *cfg.MaxObjSizeGB
	}
	return defaultMaxObjSizeGB
}
