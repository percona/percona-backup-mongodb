package pbm

import (
	"context"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// BreakingChangesMap map of versions introduced breaking changes to respective
// backup types.
// !!! Versions should be sorted in the ascending order.
var BreakingChangesMap = map[BackupType][]string{
	LogicalBackup:     {"1.5.0"},
	IncrementalBackup: {"2.1.0"},
	PhysicalBackup:    {},
}

type MongoVersion struct {
	PSMDBVersion  string `bson:"psmdbVersion,omitempty"`
	VersionString string `bson:"version"`
	Version       []int  `bson:"versionArray"`
}

func (v MongoVersion) Major() int {
	if len(v.Version) == 0 {
		return 0
	}

	return v.Version[0]
}

func GetMongoVersion(ctx context.Context, m *mongo.Client) (MongoVersion, error) {
	res := m.Database("admin").RunCommand(ctx, bson.D{{"buildInfo", 1}})
	if err := res.Err(); err != nil {
		return MongoVersion{}, err
	}

	var ver MongoVersion
	if err := res.Decode(&ver); err != nil {
		return MongoVersion{}, err
	}

	return ver, nil
}

type FeatureSupport MongoVersion

func (f FeatureSupport) PBMSupport() error {
	v := MongoVersion(f)

	if v.Version[0] == 4 && v.Version[1] == 4 {
		return nil
	}

	if (v.Version[0] == 5 || v.Version[0] == 6) && v.Version[1] == 0 {
		return nil
	}

	return errors.New("Unsupported MongoDB version. PBM works with v4.4, v5.0, v6.0")
}

func (f FeatureSupport) FullPhysicalBackup() bool {
	// PSMDB 4.2.15, 4.4.6
	v := MongoVersion(f)
	if v.PSMDBVersion == "" {
		return false
	}

	switch {
	case v.Version[0] == 4 && v.Version[1] == 2 && v.Version[2] >= 15:
		fallthrough
	case v.Version[0] == 4 && v.Version[1] == 4 && v.Version[2] >= 6:
		fallthrough
	case v.Version[0] >= 5:
		return true
	}

	return false
}

func (f FeatureSupport) IncrementalPhysicalBackup() bool {
	// PSMDB 4.2.24, 4.4.18, 5.0.14, 6.0.3
	v := MongoVersion(f)
	if v.PSMDBVersion == "" {
		return false
	}

	switch {
	case v.Version[0] == 4 && v.Version[1] == 2 && v.Version[2] >= 24:
		fallthrough
	case v.Version[0] == 4 && v.Version[1] == 4 && v.Version[2] >= 18:
		fallthrough
	case v.Version[0] == 5 && v.Version[1] == 0 && v.Version[2] >= 14:
		fallthrough
	case v.Version[0] == 6 && v.Version[1] == 0 && v.Version[2] >= 3:
		fallthrough
	case v.Version[0] >= 7:
		return true
	}

	return false
}

func (f FeatureSupport) BackupType(t BackupType) error {
	switch t {
	case PhysicalBackup:
		if !f.FullPhysicalBackup() {
			return errors.New("full physical backup works since " +
				"Percona Server for MongoDB 4.2.15, 4.4.6")
		}
	case IncrementalBackup:
		if !f.IncrementalPhysicalBackup() {
			return errors.New("incremental physical backup works since " +
				"Percona Server for MongoDB 4.2.24, 4.4.18, 5.0.14, 6.0.3")
		}
	}

	return nil
}
