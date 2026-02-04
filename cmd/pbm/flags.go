package main

import (
	"context"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

// ProfileFlag is a CLI flag that can be used to set a profile name.
// It implements the pflag.Value interface (used by Cobra).
type ProfileFlag struct {
	value *string
}

func (p *ProfileFlag) String() string {
	return p.Name()
}

func (p *ProfileFlag) Type() string {
	return "string"
}

func (p *ProfileFlag) Set(s string) error {
	if s == "" {
		return errors.New("empty profile name")
	}
	p.value = &s
	return nil
}

func (p *ProfileFlag) IsSet() bool {
	return p.value != nil
}

func (p *ProfileFlag) Value() string {
	if !p.IsSet() {
		// unprintable zero byte prefix guards against invalid use
		// unset value can never match actual profile
		return "\x00*"
	}
	if p.IsDefault() {
		return ""
	}
	return *p.value
}

func (p *ProfileFlag) Name() string {
	if !p.IsSet() {
		return "*"
	}
	return *p.value
}

func (p *ProfileFlag) IsDefault() bool {
	return p.IsSet() && *p.value == "default"
}

func (p *ProfileFlag) IsProfile() bool {
	return p.IsSet() && !p.IsDefault()
}

// Validate checks whether this is either a wildcard or a reference to valid config
func (p *ProfileFlag) Validate(ctx context.Context, conn connect.Client) error {
	if !p.IsSet() {
		return nil
	}
	return p.ValidateExists(ctx, conn)
}

func (p *ProfileFlag) ValidateExists(ctx context.Context, conn connect.Client) error {
	_, err := config.GetProfiledConfig(ctx, conn, p.Value())
	if err != nil {
		if errors.Is(err, config.ErrMissedConfig) {
			return errors.New("no config set. Set config with <pbm config>")
		}
		if errors.Is(err, config.ErrMissedConfigProfile) {
			return errors.Errorf("profile %q is not found", p.Name())
		}
		return errors.Wrap(err, "get config")
	}

	return nil
}

// NewProfileFlag creates a new profile flag with an initial value
func NewProfileFlag(v string) ProfileFlag {
	pf := ProfileFlag{}
	if v != "" {
		_ = pf.Set(v)
	}
	return pf
}

// NewProfileFlagDefault is the same as NewProfileFlag with default profile as initial value
func NewProfileFlagDefault() ProfileFlag {
	return NewProfileFlag("default")
}
