package config

import (
	"strings"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

// LifecycleConf defines the backup rotation and retention policy rules.
type LifecycleConf struct {
	Enabled     bool   `bson:"enabled" json:"enabled" yaml:"enabled"`
	Strategy    string `bson:"strategy,omitempty" json:"strategy,omitempty" yaml:"strategy,omitempty"`
	PurgeFailed bool   `bson:"purgeFailed" json:"purgeFailed" yaml:"purgeFailed"`

	MinKeep *int `bson:"minKeep,omitempty" json:"minKeep,omitempty" yaml:"minKeep,omitempty"`

	DailyRetention   int `bson:"dailyRetention" json:"dailyRetention" yaml:"dailyRetention"`
	WeeklyRetention  int `bson:"weeklyRetention" json:"weeklyRetention" yaml:"weeklyRetention"`
	WeeklyDay        int `bson:"weeklyDay" json:"weeklyDay" yaml:"weeklyDay"`
	MonthlyRetention int `bson:"monthlyRetention" json:"monthlyRetention" yaml:"monthlyRetention"`
	MonthlyDay       int `bson:"monthlyDay" json:"monthlyDay" yaml:"monthlyDay"`
}

const (
	LifecycleStrategyRolling  = "rolling"
	LifecycleStrategyCalendar = "calendar"
	DefaultLifecycleMinKeep   = 1
)

func (c *LifecycleConf) Clone() *LifecycleConf {
	if c == nil {
		return nil
	}

	rv := *c
	if c.MinKeep != nil {
		minKeep := *c.MinKeep
		rv.MinKeep = &minKeep
	}

	return &rv
}

func (c *LifecycleConf) GetStrategy() string {
	if c == nil || c.Strategy == "" {
		return LifecycleStrategyRolling
	}

	return strings.ToLower(c.Strategy)
}

func (c *LifecycleConf) GetMinKeep() int {
	if c == nil || c.MinKeep == nil {
		return DefaultLifecycleMinKeep
	}

	return *c.MinKeep
}

func ValidateLifecycle(c *LifecycleConf) error {
	if c == nil {
		return nil
	}

	strategy := c.GetStrategy()
	if strategy != LifecycleStrategyRolling && strategy != LifecycleStrategyCalendar {
		return errors.Errorf("lifecycle.strategy must be %q or %q", LifecycleStrategyRolling, LifecycleStrategyCalendar)
	}
	if c.DailyRetention < 0 {
		return errors.New("lifecycle.dailyRetention cannot be negative")
	}
	if c.WeeklyRetention < 0 {
		return errors.New("lifecycle.weeklyRetention cannot be negative")
	}
	if c.MonthlyRetention < 0 {
		return errors.New("lifecycle.monthlyRetention cannot be negative")
	}
	if c.GetMinKeep() < 0 {
		return errors.New("lifecycle.minKeep cannot be negative")
	}
	if strategy == LifecycleStrategyCalendar && c.WeeklyRetention > 0 &&
		(c.WeeklyDay < int(time.Sunday) || c.WeeklyDay > int(time.Saturday)) {
		return errors.New("lifecycle.weeklyDay must be between 0 and 6")
	}
	if strategy == LifecycleStrategyCalendar && c.MonthlyRetention > 0 &&
		(c.MonthlyDay < 1 || c.MonthlyDay > 31) {
		return errors.New("lifecycle.monthlyDay must be between 1 and 31")
	}

	return nil
}
