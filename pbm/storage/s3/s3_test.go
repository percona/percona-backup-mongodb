package s3

import (
	"gopkg.in/yaml.v2"
	"testing"
	"time"
)

func TestRetryDelayParsing(t *testing.T) {
	yamlConfig := `
retryer:
  numMaxRetries: 3
  minRetryDelay: 1000ms
  maxRetryDelay: 5m`

	var cfg Config

	err := yaml.Unmarshal([]byte(yamlConfig), &cfg)
	if err != nil {
		t.Fatalf(err.Error())
	}

	if cfg.Retryer == nil {
		t.Errorf("Retryer is nil")
	}

	if cfg.Retryer.NumMaxRetries != 3 {
		t.Errorf("Expected NumMaxRetries = %d; got 3", cfg.Retryer.NumMaxRetries)
	}

	if cfg.Retryer.MinRetryDelay != 1000*time.Millisecond {
		t.Errorf("Expected MinRetryDelay = %s; got %s", 1000*time.Millisecond, cfg.Retryer.MinRetryDelay)
	}

	if cfg.Retryer.MaxRetryDelay != 5*time.Minute {
		t.Errorf("Expected MaxRetryDelay = %s; got %s", 5*time.Minute, cfg.Retryer.MaxRetryDelay)
	}
}
