package cli

import (
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/yaml.v2"

	"github.com/percona/percona-backup-mongodb/pbm"
)

type configOpts struct {
	rsync bool
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

func (c confVals) String() (s string) {
	for _, v := range c {
		s += v.String() + "\n"
	}

	return s
}

func runConfig(cn *pbm.PBM, c *configOpts) (fmt.Stringer, error) {
	switch {
	case len(c.set) > 0:
		var o confVals
		rsnc := false
		for k, v := range c.set {
			err := cn.SetConfigVar(k, v)
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
			rsync(cn)
		}
		return o, nil
	case len(c.key) > 0:
		k, err := cn.GetConfigVar(c.key)
		if err != nil {
			return nil, errors.Wrap(err, "unable to get config key")
		}
		return confKV{c.key, fmt.Sprint(k)}, nil
	case c.rsync:
		err := rsync(cn)
		if err != nil {
			return nil, err
		}
		return outMsg{"Storage resync started"}, nil
	case len(c.file) > 0:
		buf, err := ioutil.ReadFile(c.file)
		if err != nil {
			return nil, errors.Wrap(err, "unable to read config file")
		}

		var cfg pbm.Config
		err = yaml.UnmarshalStrict(buf, &cfg)
		if err != nil {
			return nil, errors.Wrap(err, "unable to  unmarshal config file")
		}

		cCfg, err := cn.GetConfig()
		if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
			return nil, errors.Wrap(err, "unable to get current config")
		}

		err = cn.SetConfigByte(buf)
		if err != nil {
			return nil, errors.Wrap(err, "unable to set config")
		}

		// provider value may differ as it set automatically after config parsing
		cCfg.Storage.S3.Provider = cfg.Storage.S3.Provider
		// resync storage only if Storage options have changed
		if !reflect.DeepEqual(cfg.Storage, cCfg.Storage) {
			err := rsync(cn)
			if err != nil {
				return nil, err
			}
		}

		return cn.GetConfig()
	default:
		return cn.GetConfig()
	}
}

func rsync(cn *pbm.PBM) error {
	return cn.SendCmd(pbm.Cmd{
		Cmd: pbm.CmdResync,
	})
}
