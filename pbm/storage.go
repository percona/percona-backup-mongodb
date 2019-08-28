package pbm

import (
	"context"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/yaml.v2"
)

type StorageType string

const (
	StorageUndef      StorageType = ""
	StorageS3                     = "s3"
	StorageFilesystem             = "filesystem"
)

type Storage struct {
	Type       StorageType `bson:"type" json:"type" yaml:"type"`
	S3         S3          `bson:"s3,omitempty" json:"s3,omitempty" yaml:"s3,omitempty"`
	Filesystem Filesystem  `bson:"filesystem,omitempty" json:"filesystem,omitempty" yaml:"filesystem,omitempty"`
}

type S3 struct {
	Region      string      `bson:"region" json:"region" yaml:"region"`
	EndpointURL string      `bson:"endpointUrl,omitempty" json:"endpointUrl" yaml:"endpointUrl,omitempty"`
	Bucket      string      `bson:"bucket" json:"bucket" yaml:"bucket"`
	Credentials Credentials `bson:"credentials" json:"credentials,omitempty" yaml:"credentials"`
}

type Filesystem struct {
	Path string `bson:"path" json:"path" yaml:"path"`
}

type Credentials struct {
	AccessKeyID     string `bson:"access-key-id" json:"access-key-id,omitempty" yaml:"access-key-id,omitempty"`
	SecretAccessKey string `bson:"secret-access-key" json:"secret-access-key,omitempty" yaml:"secret-access-key,omitempty"`
	Vault           struct {
		Server string `bson:"server" json:"server,omitempty" yaml:"server"`
		Secret string `bson:"secret" json:"secret,omitempty" yaml:"secret"`
		Token  string `bson:"token" json:"token,omitempty" yaml:"token"`
	} `bson:"vault" json:"vault" yaml:"vault,omitempty"`
}

const defaultName = "default"

func (p *PBM) SetStorageByte(buf []byte) error {
	var stg Storage
	err := yaml.Unmarshal(buf, &stg)
	if err != nil {
		errors.Wrap(err, "unmarshal yaml")
	}
	return errors.Wrap(p.SetStorage(stg), "write to mongo")
}

func (p *PBM) SetStorage(stg Storage) error {
	_, err := p.configC.UpdateOne(
		context.Background(),
		bson.D{{"item", "config"}},
		bson.M{"$set": bson.M{"storage": map[string]Storage{defaultName: stg}}},
		options.Update().SetUpsert(true))

	return err
}

func (p *PBM) GetStorageYaml(safe bool) ([]byte, error) {
	s, err := p.GetStorage()
	if err != nil {
		errors.Wrap(err, "get from mongo")
	}

	if safe {
		if s.S3.Credentials.AccessKeyID != "" {
			s.S3.Credentials.AccessKeyID = "***"
		}
		if s.S3.Credentials.SecretAccessKey != "" {
			s.S3.Credentials.SecretAccessKey = "***"
		}
		if s.S3.Credentials.Vault.Secret != "" {
			s.S3.Credentials.Vault.Secret = "***"
		}
		if s.S3.Credentials.Vault.Token != "" {
			s.S3.Credentials.Vault.Token = "***"
		}
	}

	b, err := yaml.Marshal(s)
	return b, errors.Wrap(err, "marshal yaml")
}

func (p *PBM) GetStorage() (Storage, error) {
	var c Conf
	err := p.configC.FindOne(context.Background(), bson.D{{"item", "config"}}).Decode(&c)

	return c.Storage[defaultName], errors.Wrap(err, "")
}
