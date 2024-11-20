package log

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

const (
	cfgChangePollingCycle = 5 * time.Second
)

func (l *loggerImpl) cfgChangeMonitor(ctx context.Context, initialCfg *cfg) {
	l.Printf("start cfg change monitor for logger")
	defer l.Printf("stop cfg change monitor for logger")

	tk := time.NewTicker(cfgChangePollingCycle)
	defer tk.Stop()

	currentCfg := initialCfg

	for {
		select {
		case <-tk.C:
			lastCfg, err := getConfig(ctx, l.conn)
			if err != nil {
				if !errors.Is(err, mongo.ErrNoDocuments) {
					l.Printf("error while monitoring for pitr conf change: %v", err)
				}
				continue
			}
			if lastCfg.Epoch.Equal(currentCfg.Epoch) || lastCfg.Epoch.Before(currentCfg.Epoch) {
				continue
			}

			l.applyConfigChange(lastCfg.Logging)
			currentCfg = lastCfg

		case <-ctx.Done():
			return
		}
	}
}

func getConfig(ctx context.Context, m connect.Client) (*cfg, error) {
	res := m.ConfigCollection().FindOne(ctx, bson.D{{"profile", nil}})
	if err := res.Err(); err != nil {
		return nil, errors.Wrap(err, "get")
	}

	cfg := &cfg{}
	if err := res.Decode(&cfg); err != nil {
		return nil, errors.Wrap(err, "decode")
	}

	return cfg, nil
}

type cfg struct {
	Logging *loggingCfg         `bson:"log,omitempty" json:"log,omitempty" yaml:"log,omitempty"`
	Epoch   primitive.Timestamp `bson:"epoch" json:"-" yaml:"-"`
}

type loggingCfg struct {
	Path  string `json:"path,omitempty" bson:"path,omitempty" yaml:"path,omitempty"`
	Level string `json:"level,omitempty" bson:"level,omitempty" yaml:"level,omitempty"`
	JSON  *bool  `json:"json,omitempty" bson:"json,omitempty" yaml:"json,omitempty"`
}
