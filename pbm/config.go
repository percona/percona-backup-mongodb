package pbm

import (
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/yaml.v2"
)

// Config is a pbm config
type Config struct {
	Storage Storage `bson:"storage" json:"storage" yaml:"storage"`
}

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

// ConfKeys returns valid config keys (option names)
func ConfKeys() []string {
	return keys(reflect.TypeOf(Config{}))
}

func keys(t reflect.Type) []string {
	var v []string
	for i := 0; i < t.NumField(); i++ {
		name := strings.TrimSpace(strings.Split(t.Field(i).Tag.Get("bson"), ",")[0])
		if t.Field(i).Type.Kind() == reflect.Struct {
			for _, n := range keys(t.Field(i).Type) {
				v = append(v, name+"."+n)
			}
		} else {
			v = append(v, name)
		}
	}
	return v
}

func (p *PBM) SetConfigByte(buf []byte) error {
	var cfg Config
	err := yaml.Unmarshal(buf, &cfg)
	if err != nil {
		errors.Wrap(err, "unmarshal yaml")
	}
	return errors.Wrap(p.SetConfig(cfg), "write to db")
}

func (p *PBM) SetConfig(cfg Config) error {
	_, err := p.Conn.Database(DB).Collection(ConfigCollection).UpdateOne(
		p.ctx,
		bson.D{},
		bson.M{"$set": cfg},
		options.Update().SetUpsert(true))

	return err
}

func (p *PBM) SetConfigVar(key, val string) error {
	if !ValidateConfigKey(key) {
		return errors.New("invalid config key")
	}

	// just check if config was set
	_, err := p.GetConfigVar(key)
	if err != nil {
		if errors.Cause(err) == mongo.ErrNoDocuments {
			return errors.New("config doesn't set")
		}
		return err
	}
	_, err = p.Conn.Database(DB).Collection(ConfigCollection).UpdateOne(
		p.ctx,
		bson.D{},
		bson.M{"$set": bson.M{key: val}},
	)

	return errors.Wrap(err, "write to db")
}

// GetConfigVar returns value of given config vaiable
func (p *PBM) GetConfigVar(key string) (string, error) {
	if !ValidateConfigKey(key) {
		return "", errors.New("invalid config key")
	}

	bts, err := p.Conn.Database(DB).Collection(ConfigCollection).FindOne(p.ctx, bson.D{}).DecodeBytes()
	if err != nil {
		return "", errors.Wrap(err, "get from db")
	}
	return string(bts.Lookup(strings.Split(key, ".")...).Value), nil
}

// ValidateConfigKey checks if a config key valid
func ValidateConfigKey(k string) bool {
	for _, v := range ConfKeys() {
		if k == v {
			return true
		}
	}
	return false
}

func (p *PBM) GetConfigYaml(fieldRedaction bool) ([]byte, error) {
	c, err := p.GetConfig()
	if err != nil {
		errors.Wrap(err, "get from db")
	}

	if fieldRedaction {
		if c.Storage.S3.Credentials.AccessKeyID != "" {
			c.Storage.S3.Credentials.AccessKeyID = "***"
		}
		if c.Storage.S3.Credentials.SecretAccessKey != "" {
			c.Storage.S3.Credentials.SecretAccessKey = "***"
		}
		if c.Storage.S3.Credentials.Vault.Secret != "" {
			c.Storage.S3.Credentials.Vault.Secret = "***"
		}
		if c.Storage.S3.Credentials.Vault.Token != "" {
			c.Storage.S3.Credentials.Vault.Token = "***"
		}
	}

	b, err := yaml.Marshal(c)
	return b, errors.Wrap(err, "marshal yaml")
}

func (p *PBM) GetConfig() (Config, error) {
	var c Config
	res := p.Conn.Database(DB).Collection(ConfigCollection).FindOne(p.ctx, bson.D{})
	if res.Err() != nil {
		return Config{}, errors.Wrap(res.Err(), "get")
	}
	err := res.Decode(&c)
	return c, errors.Wrap(err, "decode")
}

func (p *PBM) GetStorage() (Storage, error) {
	c, err := p.GetConfig()
	return c.Storage, err
}
