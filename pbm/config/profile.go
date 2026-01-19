package config

import (
	"context"
	"os"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
	"github.com/percona/percona-backup-mongodb/pbm/storage/s3"
)

var (
	ProfileNameDefault     = ProfileName{value: ""}
	ProfileNameWildcard    = ProfileName{value: "*"}
	ErrProfileNameWildcard = errors.New("wildcard profile name")
)

type ProfileName struct {
	value string
}

func NewProfileName(name string) ProfileName {
	switch name {
	case "*":
		return ProfileNameWildcard
	case "", "default":
		return ProfileNameDefault
	default:
		return ProfileName{value: name}
	}
}

func (p ProfileName) DisplayName() string {
	if p.IsDefault() {
		return "default"
	}
	return p.value
}

func (p ProfileName) Name() string {
	return p.value
}

func (p ProfileName) Equals(o ProfileName) bool {
	return p.value == o.value
}

func (p ProfileName) IsWildcard() bool {
	return p.Equals(ProfileNameWildcard)
}

func (p ProfileName) IsDefault() bool {
	return p.Equals(ProfileNameDefault)
}

func (p ProfileName) IsDefaultOrWildcard() bool {
	return p.IsDefault() || p.IsWildcard()
}

func (p ProfileName) Exists(ctx context.Context, conn connect.Client) error {
	if p.IsWildcard() {
		return ErrProfileNameWildcard
	}

	_, err := GetProfiledConfig(ctx, conn, p.value)
	if err != nil {
		if errors.Is(err, ErrMissedConfig) {
			return errors.New("no config set. Set config with <pbm config>")
		}
		if errors.Is(err, ErrMissedConfigProfile) {
			return errors.Errorf("profile %q is not found", p.value)
		}
		return errors.Wrap(err, "get config")
	}

	return nil
}

func ListProfiles(ctx context.Context, m connect.Client) ([]Config, error) {
	cur, err := m.ConfigCollection().Find(ctx, bson.D{
		{"profile", true},
	})
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}

	var profiles []Config
	err = cur.All(ctx, &profiles)
	if err != nil {
		return nil, errors.Wrap(err, "decode")
	}

	return profiles, nil
}

func GetProfile(ctx context.Context, m connect.Client, name string) (*Config, error) {
	res := m.ConfigCollection().FindOne(ctx, bson.D{
		{"profile", true},
		{"name", name},
	})
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrMissedConfigProfile
		}

		return nil, errors.Wrap(err, "query")
	}

	var profile *Config
	err := res.Decode(&profile)
	if err != nil {
		return nil, errors.Wrap(err, "decode")
	}

	return profile, nil
}

func AddProfile(ctx context.Context, m connect.Client, profile *Config) error {
	if !profile.IsProfile {
		return errors.New("not a profile")
	}
	if profile.Name == "" {
		return errors.New("name is required")
	}

	if err := profile.Storage.Cast(); err != nil {
		return errors.Wrap(err, "cast storage")
	}

	if profile.Storage.Type == storage.S3 {
		// call the function for notification purpose.
		// warning about unsupported levels will be printed
		s3.SDKLogLevel(profile.Storage.S3.DebugLogLevels, os.Stderr)
	}

	_, err := m.ConfigCollection().ReplaceOne(ctx,
		bson.D{
			{"profile", true},
			{"name", profile.Name},
		},
		profile,
		options.Replace().SetUpsert(true))
	if err != nil {
		return errors.Wrap(err, "save profile")
	}

	return nil
}

func RemoveProfile(ctx context.Context, m connect.Client, name string) error {
	if name == "" {
		return errors.New("name is required")
	}

	_, err := m.ConfigCollection().DeleteOne(ctx, bson.D{
		{"profile", true},
		{"name", name},
	})
	if err != nil {
		return errors.Wrap(err, "query")
	}

	return nil
}
