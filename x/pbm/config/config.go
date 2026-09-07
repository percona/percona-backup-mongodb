// PBM 2.x package
package config

import (
	"fmt"
	"io"
	"maps"

	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"gopkg.in/yaml.v2"

	"github.com/percona/percona-backup-mongodb/x/pbm/compress"
	"github.com/percona/percona-backup-mongodb/x/pbm/config/fs"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/storage"
)

var (
	ErrUnkownStorageType   = errors.New("unknown storage type")
	ErrMissedConfig        = errors.New("missed config")
	ErrMissedConfigProfile = errors.New("missed config profile")
	ErrUnsetConfigPath     = bsoncore.ErrElementNotFound
)

// Config is a pbm config
type Config struct {
	Name      string `bson:"name,omitempty" json:"name,omitempty" yaml:"name,omitempty"`
	IsProfile bool   `bson:"profile,omitempty" json:"profile,omitempty" yaml:"profile,omitempty"`

	Storage StorageConf  `bson:"storage" json:"storage" yaml:"storage"`
	PITR    *PITRConf    `bson:"pitr,omitempty" json:"pitr,omitempty" yaml:"pitr,omitempty"`
	Backup  *BackupConf  `bson:"backup,omitempty" json:"backup,omitempty" yaml:"backup,omitempty"`
	Restore *RestoreConf `bson:"restore,omitempty" json:"restore,omitempty" yaml:"restore,omitempty"`
}

func (c *Config) String() string {
	b, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Sprintln("error:", err)
	}

	return string(b)
}

// IsSameStorage reports whether new configuration points at the same storage
// instance as old configuration.
func (c *Config) IsSameStorage(other *Config) bool {
	return c.Storage.IsSameStorage(&other.Storage)
}

// Parse reads a YAML config document from r. It rejects unknown fields and
// validates the storage section.
func Parse(r io.Reader) (*Config, error) {
	cfg := &Config{}

	dec := yaml.NewDecoder(r)
	dec.SetStrict(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, errors.Wrap(err, "decode")
	}

	if err := cfg.Storage.Cast(); err != nil {
		return nil, errors.Wrap(err, "storage cast")
	}

	return cfg, nil
}

// Priority contains priority values for cluster members.
// It is used for specifying Backup and PITR configuration priorities.
type Priority map[string]float64

// PITRConf is a Point-In-Time Recovery options
//
//nolint:lll
type PITRConf struct {
	Enabled          bool                     `bson:"enabled" json:"enabled" yaml:"enabled"`
	OplogSpanMin     float64                  `bson:"oplogSpanMin,omitempty" json:"oplogSpanMin,omitempty" yaml:"oplogSpanMin,omitempty"`
	OplogOnly        bool                     `bson:"oplogOnly,omitempty" json:"oplogOnly,omitempty" yaml:"oplogOnly,omitempty"`
	Priority         Priority                 `bson:"priority,omitempty" json:"priority,omitempty" yaml:"priority,omitempty"`
	Compression      compress.CompressionType `bson:"compression,omitempty" json:"compression,omitempty" yaml:"compression,omitempty"`
	CompressionLevel *int                     `bson:"compressionLevel,omitempty" json:"compressionLevel,omitempty" yaml:"compressionLevel,omitempty"`
}

func (cfg *PITRConf) Clone() *PITRConf {
	if cfg == nil {
		return nil
	}

	rv := *cfg
	rv.Priority = maps.Clone(cfg.Priority)
	if cfg.CompressionLevel != nil {
		a := *cfg.CompressionLevel
		rv.CompressionLevel = &a
	}

	return &rv
}

// StorageConf is a configuration of the backup storage
type StorageConf struct {
	Type       storage.Type `bson:"type" json:"type" yaml:"type"`
	Filesystem *fs.Config   `bson:"filesystem,omitempty" json:"filesystem,omitempty" yaml:"filesystem,omitempty"`
}

func (s *StorageConf) Clone() *StorageConf {
	if s == nil {
		return nil
	}

	rv := &StorageConf{
		Type: s.Type,
	}

	switch s.Type {
	case storage.Filesystem:
		rv.Filesystem = s.Filesystem.Clone()
	}

	return rv
}

func (s *StorageConf) Equal(other *StorageConf) bool {
	if s.Type != other.Type {
		return false
	}

	switch s.Type {
	case storage.Filesystem:
		return s.Filesystem.Equal(other.Filesystem)
	}

	return false
}

// IsSameStorage returns true if specified config params describes
// the same instance of the storage.
// It ignores storage properties, and just compare parts that specifies
// the storage instance.
func (s *StorageConf) IsSameStorage(other *StorageConf) bool {
	if s.Type != other.Type {
		return false
	}

	switch s.Type {
	case storage.Filesystem:
		return s.Filesystem.IsSameStorage(other.Filesystem)
	}
	return false
}

func (s *StorageConf) Cast() error {
	switch s.Type {
	case storage.Filesystem:
		return s.Filesystem.Cast()
	}

	return errors.Wrap(ErrUnkownStorageType, string(s.Type))
}

func (s *StorageConf) Typ() string {
	switch s.Type {
	case storage.Filesystem:
		return "FS"
	default:
		return "Unknown"
	}
}

func (s *StorageConf) Path() string {
	path := ""
	switch s.Type {
	case storage.Filesystem:
		path = s.Filesystem.Path
	}

	return path
}

func (s *StorageConf) Region() string {
	region := ""

	switch s.Type {
	}
	return region
}

// RestoreConf is config options for the restore
//
//nolint:lll
type RestoreConf struct {
	// Logical restore
	//
	// num of documents to buffer
	BatchSize              int               `bson:"batchSize" json:"batchSize,omitempty" yaml:"batchSize,omitempty"`
	NumInsertionWorkers    int               `bson:"numInsertionWorkers" json:"numInsertionWorkers,omitempty" yaml:"numInsertionWorkers,omitempty"`
	NumParallelCollections int               `bson:"numParallelCollections" json:"numParallelCollections,omitempty" yaml:"numParallelCollections,omitempty"`
	NumParallelFiles       int               `bson:"numParallelFiles" json:"numParallelFiles,omitempty" yaml:"numParallelFiles,omitempty"`
	IndexCommitQuorum      IndexCommitQuorum `bson:"indexCommitQuorum,omitempty" json:"indexCommitQuorum,omitempty" yaml:"indexCommitQuorum,omitempty"`

	// NumDownloadWorkers sets the num of goroutine would be requesting chunks
	// during the download. By default, it's set to GOMAXPROCS.
	NumDownloadWorkers int `bson:"numDownloadWorkers" json:"numDownloadWorkers,omitempty" yaml:"numDownloadWorkers,omitempty"`
	// MaxDownloadBufferMb sets the max size of the in-memory buffer that is used
	// to download files from the storage.
	MaxDownloadBufferMb int `bson:"maxDownloadBufferMb" json:"maxDownloadBufferMb,omitempty" yaml:"maxDownloadBufferMb,omitempty"`
	DownloadChunkMb     int `bson:"downloadChunkMb" json:"downloadChunkMb,omitempty" yaml:"downloadChunkMb,omitempty"`

	// MongodLocation sets the location of mongod used for internal runs during
	// physical restore. Will try $PATH/mongod if not set.
	MongodLocation    string            `bson:"mongodLocation" json:"mongodLocation,omitempty" yaml:"mongodLocation,omitempty"`
	MongodLocationMap map[string]string `bson:"mongodLocationMap" json:"mongodLocationMap,omitempty" yaml:"mongodLocationMap,omitempty"`

	FallbackEnabled *bool `bson:"fallbackEnabled,omitempty" json:"fallbackEnabled,omitempty" yaml:"fallbackEnabled,omitempty"`
	AllowPartlyDone *bool `bson:"allowPartlyDone,omitempty" json:"allowPartlyDone,omitempty" yaml:"allowPartlyDone,omitempty"`
}

//nolint:lll
type BackupConf struct {
	OplogSpanMin     float64                  `bson:"oplogSpanMin" json:"oplogSpanMin" yaml:"oplogSpanMin"`
	Priority         Priority                 `bson:"priority,omitempty" json:"priority,omitempty" yaml:"priority,omitempty"`
	Compression      compress.CompressionType `bson:"compression,omitempty" json:"compression,omitempty" yaml:"compression,omitempty"`
	CompressionLevel *int                     `bson:"compressionLevel,omitempty" json:"compressionLevel,omitempty" yaml:"compressionLevel,omitempty"`

	NumParallelCollections int `bson:"numParallelCollections" json:"numParallelCollections,omitempty" yaml:"numParallelCollections,omitempty"`
	NumParallelFiles       int `bson:"numParallelFiles" json:"numParallelFiles,omitempty" yaml:"numParallelFiles,omitempty"`
}
