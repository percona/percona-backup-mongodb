package pbm

import (
	"time"
)

type Credentials struct {
	AccessKeyID     string `yaml:"access-key-id"`
	SecretAccessKey string `yaml:"secret-access-key"`
	Vault           struct {
		Server string `yaml:"server"`
		Secret string `yaml:"secret"`
		Token  string `yaml:"token"`
	} `yaml:"vault"`
}

type S3 struct {
	Region      string      `yaml:"region"`
	EndpointURL string      `yaml:"endpointUrl"`
	Bucket      string      `yaml:"bucket"`
	Credentials Credentials `yaml:"credentials"`
}

type Filesystem struct {
	Path string `yaml:"path"`
}

type StorageType string

const (
	StorageS3         StorageType = "s3"
	StorageFilesystem             = "filesystem"
)

type Storage struct {
	Type       StorageType `yaml:"type"`
	S3         S3          `yaml:"s3,omitempty"`
	Filesystem Filesystem  `yaml:"filesystem"`
}

type StorageInfo struct {
	Type       string
	S3         S3
	Filesystem Filesystem
}

type Storages struct {
	Storages   map[string]Storage
	lastUpdate time.Time
	filename   string
}

type CompressionType int

const (
	CompressionTypeUndef CompressionType = iota
	CompressionTypeNo
	CompressionTypeGZIP
	CompressionTypeSNAPPY
	CompressionTypeLZ4
)
