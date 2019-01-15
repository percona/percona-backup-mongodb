package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/percona/percona-backup-mongodb/internal/utils"
	"github.com/pkg/errors"
	yaml "gopkg.in/yaml.v2"
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

type Storage struct {
	Type       string `yaml:"type"`
	S3         S3     `yaml:"s3,omitempty"`
	Filesystem Filesystem
}

type StorageInfo struct {
	Type       string
	S3         S3
	Filesystem Filesystem
}

type Storages map[string]Storage

func (s Storage) Info() StorageInfo {
	return StorageInfo{
		Type: s.Type,
		S3: S3{
			Region:      s.S3.Region,
			EndpointURL: s.S3.EndpointURL,
			Bucket:      s.S3.Bucket,
		},
		Filesystem: Filesystem{
			Path: s.Filesystem.Path,
		},
	}
}

func NewStorageBackends() Storages {
	return make(map[string]Storage)
}

func NewStorageBackendsFromYaml(filename string) (Storages, error) {
	filename = utils.Expand(filename)

	if _, err := os.Stat(filename); err != nil {
		return nil, errors.Wrapf(err, "cannot open file %s", filename)
	}

	buf, err := ioutil.ReadFile(filepath.Clean(filename))
	if err != nil {
		return nil, errors.Wrap(err, "cannot load configuration from file")
	}

	s := make(map[string]Storage)

	if err = yaml.Unmarshal(buf, s); err != nil {
		return nil, errors.Wrapf(err, "cannot unmarshal yaml file %s: %s", filename, err)
	}

	return s, nil
}

func (s Storages) Exists(name string) bool {
	_, ok := s[name]
	return ok
}

func (s Storages) StorageInfo(name string) (StorageInfo, error) {
	si, ok := s[name]
	if !ok {
		return StorageInfo{}, fmt.Errorf("Storage name %s doesn't exists", name)
	}
	return si.Info(), nil
}

func (s Storages) StoragesInfo() map[string]StorageInfo {
	si := make(map[string]StorageInfo)
	for name, storage := range s {
		si[name] = storage.Info()
	}
	return si
}
