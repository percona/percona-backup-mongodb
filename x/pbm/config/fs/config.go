package fs

import (
	"reflect"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
)

//nolint:lll
type Config struct {
	Path            string   `bson:"path" json:"path" yaml:"path"`
	MaxObjSizeGB    *float64 `bson:"maxObjSizeGB,omitempty" json:"maxObjSizeGB,omitempty" yaml:"maxObjSizeGB,omitempty"`
	BackupBuffSize  int      `bson:"backupBuffSize,omitempty" json:"backupBuffSize,omitempty" yaml:"backupBuffSize,omitempty"`
	RestoreBuffSize int      `bson:"restoreBuffSize,omitempty" json:"restoreBuffSize,omitempty" yaml:"restoreBuffSize,omitempty"`
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
	if cfg == nil || other == nil {
		return cfg == other
	}
	if cfg.Path != other.Path {
		return false
	}
	if !reflect.DeepEqual(cfg.MaxObjSizeGB, other.MaxObjSizeGB) {
		return false
	}

	return true
}

// IsSameStorage identifies the same instance of the FS storage.
func (cfg *Config) IsSameStorage(other *Config) bool {
	if cfg == nil || other == nil {
		return cfg == other
	}

	if cfg.Path != other.Path {
		return false
	}

	return true
}

func (cfg *Config) Cast() error {
	if cfg == nil {
		return errors.New("missing filesystem configuration with filesystem storage type")
	}
	if cfg.Path == "" {
		return errors.New("path can't be empty")
	}

	return nil
}

func (cfg *Config) GetBackupBuffSize() int {
	if cfg.BackupBuffSize <= 0 {
		return 0
	}
	return normalizeBuffSize(cfg.BackupBuffSize)
}

func (cfg *Config) GetRestoreBuffSize() int {
	if cfg.RestoreBuffSize <= 0 {
		return 0
	}
	return normalizeBuffSize(cfg.RestoreBuffSize)
}

func normalizeBuffSize(sz int) int {
	// normalize buff size within range: 32KiB - 10MiB
	buffSize := max(32*1024, sz)
	buffSize = min(10*1024*1024, buffSize)

	return buffSize
}
