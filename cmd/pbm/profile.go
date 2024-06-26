package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/sdk"
)

type showConfigProfileOptions struct {
	name string
}

type addConfigProfileOptions struct {
	name string
	file *os.File
	sync bool
	wait bool
}

type removeConfigProfileOptions struct {
	name string
	wait bool
}

type syncConfigProfileOptions struct {
	name  string
	all   bool
	clear bool
	wait  bool
}

type configProfileList struct {
	Profiles []config.Config `json:"profiles"`
}

func (l configProfileList) String() string {
	if len(l.Profiles) == 0 {
		return ""
	}

	sb := strings.Builder{}
	sb.WriteString(l.Profiles[0].String())
	for _, profile := range l.Profiles[1:] {
		sb.WriteString("---\n")
		sb.WriteString(profile.String())
	}

	return sb.String()
}

func handleListConfigProfiles(ctx context.Context, pbm sdk.Client) (fmt.Stringer, error) {
	profiles, err := pbm.GetAllConfigProfiles(ctx)
	if err != nil {
		return nil, err
	}
	if profiles == nil {
		// (for JSON) to have {"profiles":[]} instead of {"profiles":null}
		profiles = []config.Config{}
	}

	return configProfileList{profiles}, nil
}

func handleShowConfigProfiles(
	ctx context.Context,
	pbm sdk.Client,
	opts showConfigProfileOptions,
) (fmt.Stringer, error) {
	if opts.name == "" {
		return nil, errors.New("argument `profile-name` should not be empty")
	}

	profile, err := pbm.GetConfigProfile(ctx, opts.name)
	if err != nil {
		if errors.Is(err, config.ErrMissedConfigProfile) {
			err = errors.Errorf("profile %q is not found", opts.name)
		}
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
		return nil, errors.New("argument `profile-name` should not be empty")
	}

	_, err := pbm.GetConfig(ctx)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errors.New("PBM is not configured")
		}
		return nil, errors.Wrap(err, "get config")
	}

	cfg, err := config.Parse(opts.file)
	if err != nil {
		return nil, errors.Wrap(err, "parse config")
	}

	cid, err := pbm.AddConfigProfile(ctx, opts.name, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "add config profile")
	}

	if opts.wait {
		err = sdk.WaitForAddProfile(ctx, pbm, cid)
		if err != nil {
			return nil, errors.Wrap(err, "wait")
		}
	}

	if opts.sync {
		cid, err := pbm.SyncFromExternalStorage(ctx, opts.name)
		if err != nil {
			return nil, errors.Wrap(err, "sync")
		}

		if opts.wait {
			err = sdk.WaitForResync(ctx, pbm, cid)
			if err != nil {
				return nil, errors.Wrap(err, "wait")
			}
		}
	}

	return &outMsg{"OK"}, nil
}

func handleRemoveConfigProfile(
	ctx context.Context,
	pbm sdk.Client,
	opts removeConfigProfileOptions,
) (fmt.Stringer, error) {
	if opts.name == "" {
		return nil, errors.New("argument `profile-name` should not be empty")
	}

	_, err := pbm.GetConfigProfile(ctx, opts.name)
	if err != nil {
		if errors.Is(err, config.ErrMissedConfigProfile) {
			err = errors.Errorf("profile %q is not found", opts.name)
		}
		return nil, err
	}

	cid, err := pbm.RemoveConfigProfile(ctx, opts.name)
	if err != nil {
		return nil, errors.Wrap(err, "sdk: remove config profile")
	}

	if opts.wait {
		err = sdk.WaitForRemoveProfile(ctx, pbm, cid)
		if err != nil {
			return nil, errors.Wrap(err, "wait")
		}
	}

	return &outMsg{"OK"}, nil
}

func handleSyncConfigProfile(
	ctx context.Context,
	pbm sdk.Client,
	opts syncConfigProfileOptions,
) (fmt.Stringer, error) {
	if !opts.all && opts.name == "" {
		return nil, errors.New("<profile-name> or --all must be provided")
	}
	if opts.all && opts.name != "" {
		return nil, errors.New("ambiguous: <profile-name> and --all are provided")
	}

	var err error
	var cid sdk.CommandID

	if opts.clear {
		if opts.all {
			cid, err = pbm.ClearSyncFromAllExternalStorages(ctx)
		} else {
			cid, err = pbm.ClearSyncFromExternalStorage(ctx, opts.name)
		}
	} else {
		if opts.all {
			cid, err = pbm.SyncFromAllExternalStorages(ctx)
		} else {
			cid, err = pbm.SyncFromExternalStorage(ctx, opts.name)
		}
	}
	if err != nil {
		return nil, errors.Wrap(err, "sync from storage")
	}

	if opts.wait {
		err = sdk.WaitForResync(ctx, pbm, cid)
		if err != nil {
			return nil, errors.Wrap(err, "wait")
		}
	}

	return &outMsg{"OK"}, nil
}
