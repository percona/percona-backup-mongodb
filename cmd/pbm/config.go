package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm/config"
	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/sdk"
)

type configOpts struct {
	rsync    bool
	wait     bool
	waitTime time.Duration
	list     bool
	file     string
	set      map[string]string
	key      string
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

		if c.waitTime > time.Second {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, c.waitTime)
			defer cancel()
		}

		err = sdk.WaitForResync(ctx, pbm, cid)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				err = errWaitTimeout
			}

			return nil, errors.Wrapf(err, "waiting for resync [opid %q]", cid)
		}

		return outMsg{"Storage resync finished"}, nil
	case len(c.file) > 0:
		var err error
		var newCfg *config.Config

		if c.file == "-" {
			newCfg, err = config.Parse(os.Stdin)
		} else {
			newCfg, err = readConfigFromFile(c.file)
		}
		if err != nil {
			return nil, errors.Wrap(err, "unable to get new config")
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

func readConfigFromFile(filename string) (*config.Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrapf(err, "open %q", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("close: %v", err)
		}
	}()

	return config.Parse(file)
}
