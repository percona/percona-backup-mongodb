package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/sdk"
)

type descConfigProfileOptions struct {
	name string
}

type addConfigProfileOptions struct {
	name string
	file string
}

type removeConfigProfileOptions struct {
	name string
	wait bool
}

type configProfileList struct {
	configs []config.Config
}

func (l configProfileList) String() string {
	if len(l.configs) == 0 {
		return ""
	}

	sb := strings.Builder{}
	sb.WriteString(l.configs[0].String())
	for _, profile := range l.configs[1:] {
		sb.WriteString("---\n")
		sb.WriteString(profile.String())
	}

	return sb.String()
}

func handleListConfigProfiles(ctx context.Context, pbm sdk.Client) (fmt.Stringer, error) {
	profiles, err := pbm.ListConfigProfiles(ctx)
	if err != nil {
		return nil, err
	}

	return configProfileList{profiles}, nil
}

func handleDescibeConfigProfiles(
	ctx context.Context,
	pbm sdk.Client,
	opts descConfigProfileOptions,
) (fmt.Stringer, error) {
	if opts.name == "" {
		return nil, errors.New("name is required")
	}

	profile, err := pbm.GetConfigProfile(ctx, opts.name)
	if err != nil {
		return nil, err
	}

	return profile, nil
}

func handleAddConfigProfile(
	ctx context.Context,
	pbm sdk.Client,
	opts addConfigProfileOptions,
) (fmt.Stringer, error) {
	if opts.name == "" {
		return nil, errors.New("name is required")
	}

	var err error
	var cfg *config.Config
	if opts.file == "-" {
		cfg, err = config.Parse(os.Stdin)
	} else {
		cfg, err = readConfigFromFile(opts.file)
	}
	if err != nil {
		return nil, errors.Wrap(err, "unable to get new config")
	}

	_, err = pbm.AddConfigProfile(ctx, opts.name, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "add config profile")
	}

	return &outMsg{"OK"}, nil
}

func handleRemoveConfigProfile(
	ctx context.Context,
	pbm sdk.Client,
	opts removeConfigProfileOptions,
) (fmt.Stringer, error) {
	if opts.name == "" {
		return nil, errors.New("name is required")
	}

	_, err := pbm.RemoveConfigProfile(ctx, opts.name)
	if err != nil {
		return nil, errors.Wrap(err, "sdk: remove config profile")
	}

	return &outMsg{"OK"}, nil
}
