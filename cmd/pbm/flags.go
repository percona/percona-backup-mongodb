package main

import (
	"context"
	"fmt"
	"slices"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

var ErrNotAllowedProfileRef = errors.New("profile value not allowed")

// ProfileFlag is a CLI flag that can be used to set a profile name.
// It implements the pflag.Value interface (used by Cobra).
type ProfileFlag struct {
	Value    config.ProfileRef
	excludes []config.ProfileRef
}

func (p *ProfileFlag) String() string {
	return fmt.Sprint(p.Value)
}

func (p *ProfileFlag) Set(s string) error {
	v := config.ProfileRef(s)
	if slices.ContainsFunc(p.excludes, v.SameAs) {
		return ErrNotAllowedProfileRef
	}
	p.Value = v
	return nil
}

func (p *ProfileFlag) Type() string {
	return "string"
}

// Validate checks whether this is either a wildcard or a reference to valid config
func (p *ProfileFlag) Validate(ctx context.Context, conn connect.Client) error {
	// wildcard is valid
	if p.Value.IsAll() {
		return nil
	}

	// otherwise check if config exists
	_, err := config.GetProfiledConfig(ctx, conn, p.Value.Name())
	if err != nil {
		if errors.Is(err, config.ErrMissedConfig) {
			return errors.New("no config set. Set config with <pbm config>")
		}
		if errors.Is(err, config.ErrMissedConfigProfile) {
			return errors.Errorf("profile %q is not found", p.Value.Name())
		}
		return errors.Wrap(err, "get config")
	}

	return nil
}

// NewProfileFlag creates a new profile flag with an initial value
func NewProfileFlag(v config.ProfileRef, excludes ...config.ProfileRef) ProfileFlag {
	return ProfileFlag{Value: v, excludes: excludes}
}

// NewProfileFlagD is the same as NewProfileFlag with default profile as initial value
func NewProfileFlagD(excludes ...config.ProfileRef) ProfileFlag {
	return NewProfileFlag(config.ProfileRefDefault, excludes...)
}

// NewProfileFlagA is the same as NewProfileFlag with wildcard as initial value
func NewProfileFlagA(excludes ...config.ProfileRef) ProfileFlag {
	return NewProfileFlag(config.ProfileRefAll, excludes...)
}

// NewProfileFlagE is the same as NewProfileFlag but disallows default and wildcard references
func NewProfileFlagE(name string) ProfileFlag {
	return NewProfileFlag(config.ProfileRef(name), config.ProfileRefDefault, config.ProfileRefAll)
}
