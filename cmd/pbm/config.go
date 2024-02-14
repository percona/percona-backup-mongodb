package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/yaml.v2"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/sdk"
)

const resyncWaitDuration = 30 * time.Second

type configOpts struct {
	rsync bool
	wait  bool
	list  bool
	file  string
	set   map[string]string
	key   string
}

type confKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (c confKV) String() string {
	return fmt.Sprintf("[%s=%s]", c.Key, c.Value)
}

type confVals []confKV

func (c confVals) String() string {
	s := ""
	for _, v := range c {
		s += v.String() + "\n"
	}

	return s
}

func runConfig(ctx context.Context, conn connect.Client, pbm sdk.Client, c *configOpts) (fmt.Stringer, error) {
	switch {
	case len(c.set) > 0:
		var o confVals
		rsnc := false
		for k, v := range c.set {
			err := config.SetConfigVar(ctx, conn, k, v)
			if err != nil {
				return nil, errors.Wrapf(err, "set %s", k)
			}
			o = append(o, confKV{k, v})

			path := strings.Split(k, ".")
			if !rsnc && len(path) > 0 && path[0] == "storage" {
				rsnc = true
			}
		}
		if rsnc {
			if _, err := pbm.SyncFromStorage(ctx); err != nil {
				return nil, errors.Wrap(err, "resync")
			}
		}
		return o, nil
	case len(c.key) > 0:
		k, err := config.GetConfigVar(ctx, conn, c.key)
		if err != nil {
			return nil, errors.Wrap(err, "unable to get config key")
		}
		return confKV{c.key, fmt.Sprint(k)}, nil
	case c.rsync:
		cid, err := pbm.SyncFromStorage(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "resync")
		}

		if !c.wait {
			return outMsg{"Storage resync started"}, nil
		}

		ctx, cancel := context.WithTimeout(ctx, resyncWaitDuration)
		defer cancel()

		err = sdk.WaitForResync(ctx, pbm, cid)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				err = errors.New("timeout")
			}
			return nil, errors.Wrapf(err, "waiting for resync [opid %q]", cid)
		}

		return outMsg{"Storage resync finished"}, nil
	case len(c.file) > 0:
		var buf []byte
		var err error

		if c.file == "-" {
			buf, err = io.ReadAll(os.Stdin)
		} else {
			buf, err = os.ReadFile(c.file)
		}
		if err != nil {
			return nil, errors.Wrap(err, "unable to read config file")
		}

		var newCfg config.Config
		err = yaml.UnmarshalStrict(buf, &newCfg)
		if err != nil {
			return nil, errors.Wrap(err, "unable to  unmarshal config file")
		}

		oldCfg, err := pbm.GetConfig(ctx)
		if err != nil {
			if !errors.Is(err, mongo.ErrNoDocuments) {
				return nil, errors.Wrap(err, "unable to get current config")
			}
			oldCfg = &config.Config{}
		}

		if err := config.SetConfig(ctx, conn, newCfg); err != nil {
			return nil, errors.Wrap(err, "unable to set config: write to db")
		}

		// provider value may differ as it set automatically after config parsing
		oldCfg.Storage.S3.Provider = newCfg.Storage.S3.Provider
		// resync storage only if Storage options have changed
		if !reflect.DeepEqual(newCfg.Storage, oldCfg.Storage) {
			if _, err := pbm.SyncFromStorage(ctx); err != nil {
				return nil, errors.Wrap(err, "resync")
			}
		}

		return newCfg, nil
	}

	return pbm.GetConfig(ctx)
}
