// Package log builds the pbmx zap logger from the logging options.
package log

import (
	"slices"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Supported log outputs. Any other output is treated as a file path.
const (
	StdoutOutput = "stdout"
	StderrOutput = "stderr"
)

// Supported log formats.
const (
	JSONFormat    = "json"
	ConsoleFormat = "console"
)

// Defaults for the logging options.
const (
	DefaultLevel  = "info"
	DefaultFormat = JSONFormat
)

// supportedLevels are the levels accepted in Config.Level.
var supportedLevels = []zapcore.Level{
	zapcore.DebugLevel,
	zapcore.InfoLevel,
	zapcore.WarnLevel,
	zapcore.ErrorLevel,
}

// Config holds the logging options.
type Config struct {
	// Outputs lists the log targets: "stdout", "stderr" or a file path. Default is: stderr.
	Outputs []string
	// Level is the minimal enabled level: debug, info, warn or error. Default is: info.
	Level string
	// Format is the log encoding: json or console. Default is: json.
	Format string
}

// New builds a logger from cfg.
func New(cfg Config) (*zap.Logger, error) {
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, errors.Wrap(err, "parse log level")
	}
	format, err := parseFormat(cfg.Format)
	if err != nil {
		return nil, errors.Wrap(err, "parse log format")
	}
	outputs := outputPaths(cfg.Outputs)

	zcfg := zap.NewProductionConfig()
	zcfg.Level = zap.NewAtomicLevelAt(level)
	zcfg.Encoding = format
	zcfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zcfg.OutputPaths = outputs
	zcfg.ErrorOutputPaths = outputs
	// an agent's log is the record of what a backup/restore did, so don't
	// drop repeated entries.
	zcfg.Sampling = nil

	l, err := zcfg.Build()
	if err != nil {
		return nil, errors.Wrap(err, "build logger")
	}
	return l, nil
}

func parseLevel(s string) (zapcore.Level, error) {
	if s == "" {
		s = DefaultLevel
	}
	l, err := zapcore.ParseLevel(s)
	if err != nil {
		return 0, errors.Wrapf(err, "unknown log level %q, supported: debug, info, warn, error", s)
	}
	if !slices.Contains(supportedLevels, l) {
		return 0, errors.Errorf("unknown log level %q, supported: debug, info, warn, error", s)
	}
	return l, nil
}

func parseFormat(s string) (string, error) {
	switch s {
	case "":
		return DefaultFormat, nil
	case JSONFormat, ConsoleFormat:
		return s, nil
	default:
		return "", errors.Errorf("unknown log format %q, supported: json, console", s)
	}
}

// outputPaths maps the outputs to zap sink paths, dropping duplicates.
func outputPaths(outputs []string) []string {
	if len(outputs) == 0 {
		return []string{StderrOutput}
	}

	paths := make([]string, 0, len(outputs))
	for _, o := range outputs {
		if !slices.Contains(paths, o) {
			paths = append(paths, o)
		}
	}
	return paths
}
