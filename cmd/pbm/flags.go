package main

import (
	"context"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

const (
	ProfileValueDefault  = ""
	ProfileNameDefault   = "default"
	ProfileValueWildcard = "*"
	ProfileNameWildcard  = ProfileValueWildcard
)

// ProfileFlag is a CLI flag that can be used to set a profile name.
// It implements the pflag.Value interface (used by Cobra).
type ProfileFlag struct {
	value string
}

func (p *ProfileFlag) String() string {
	return p.Value()
}

func (p *ProfileFlag) Type() string {
	return "string"
}

func (p *ProfileFlag) Set(s string) error {
	switch s {
	case ProfileNameWildcard:
		p.value = ProfileValueWildcard
	case ProfileValueDefault, ProfileNameDefault:
		p.value = ProfileValueDefault
	default:
		p.value = s
	}
	return nil
}

func (p *ProfileFlag) Value() string {
	return p.value
}

func (p *ProfileFlag) IsWildcard() bool {
	return p.value == ProfileValueWildcard
}

func (p *ProfileFlag) IsDefault() bool {
	return p.value == ProfileValueDefault
}

func (p *ProfileFlag) IsDefaultOrWildcard() bool {
	return p.IsDefault() || p.IsWildcard()
}

func (p *ProfileFlag) DisplayName() string {
	if p.IsDefault() {
		return ProfileNameDefault
	}
	if p.IsWildcard() {
		return ProfileNameWildcard
	}
	return p.value
}

// Validate checks whether this is either a wildcard or a reference to valid config
func (p *ProfileFlag) Validate(ctx context.Context, conn connect.Client) error {
	if p.IsWildcard() {
		return nil
	}
	return p.ValidateExists(ctx, conn)
}

func (p *ProfileFlag) ValidateExists(ctx context.Context, conn connect.Client) error {
	_, err := config.GetProfiledConfig(ctx, conn, p.value)
	if err != nil {
		if errors.Is(err, config.ErrMissedConfig) {
			return errors.New("no config set. Set config with <pbm config>")
		}
		if errors.Is(err, config.ErrMissedConfigProfile) {
			return errors.Errorf("profile %q is not found", p.value)
		}
		return errors.Wrap(err, "get config")
	}

	return nil
}

// NewProfileFlag creates a new profile flag with an initial value
func NewProfileFlag(v string) ProfileFlag {
	pf := ProfileFlag{}
	_ = pf.Set(v)
	return pf
}

// NewProfileFlagDefault is the same as NewProfileFlag with default profile as initial value
func NewProfileFlagDefault() ProfileFlag {
	return NewProfileFlag(ProfileValueDefault)
}

// NewProfileFlagWildcard is the same as NewProfileFlag with wildcard as initial value
func NewProfileFlagWildcard() ProfileFlag {
	return NewProfileFlag(ProfileValueWildcard)
}
