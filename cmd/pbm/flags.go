package main

import (
	"context"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
)

// ProfileFlag is a CLI flag that can be used to set a profile name.
// It implements the pflag.Value interface (used by Cobra).
type ProfileFlag struct {
	value config.ProfileName
}

func (p *ProfileFlag) String() string {
	return p.value.DisplayName()
}

func (p *ProfileFlag) Set(s string) error {
	v := config.NewProfileName(s)
	p.value = v
	return nil
}

func (p *ProfileFlag) Type() string {
	return "string"
}

func (p *ProfileFlag) Value() config.ProfileName {
	return p.value
}

// Validate checks whether this is either a wildcard or a reference to valid config
func (p *ProfileFlag) Validate(ctx context.Context, conn connect.Client) error {
	if p.value.IsWildcard() {
		return nil
	}

	return p.value.Exists(ctx, conn)
}

// NewProfileFlag creates a new profile flag with an initial value
func NewProfileFlag(v config.ProfileName) ProfileFlag {
	return ProfileFlag{value: v}
}

// NewProfileFlagDefault is the same as NewProfileFlag with default profile as initial value
func NewProfileFlagDefault() ProfileFlag {
	return NewProfileFlag(config.ProfileNameDefault)
}

// NewProfileFlagWildcard is the same as NewProfileFlag with wildcard as initial value
func NewProfileFlagWildcard() ProfileFlag {
	return NewProfileFlag(config.ProfileNameWildcard)
}
