package disco

import (
	"log"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// newSerfStdLog returns a *log.Logger for serf and memberlist that logs
// through lg. Their lines start with a "[LEVEL] " tag; it sets the entry's
// level and is cut from the message.
func newSerfStdLog(lg *zap.Logger) *log.Logger {
	// the caller and stacktrace would point at serfLogWriter, not at serf
	lg = lg.WithOptions(zap.WithCaller(false), zap.AddStacktrace(zapcore.InvalidLevel))
	return log.New(serfLogWriter{lg: lg}, "", 0)
}

// serfLogWriter turns serf and memberlist log lines into zap entries.
type serfLogWriter struct {
	lg *zap.Logger
}

func (w serfLogWriter) Write(p []byte) (int, error) {
	lvl, msg := parseSerfLine(string(p))
	if ce := w.lg.Check(lvl, msg); ce != nil {
		ce.Write()
	}
	return len(p), nil
}

// parseSerfLine splits a "[LEVEL] message" line into its zap level and
// message. A line without a known tag is logged at info as is.
func parseSerfLine(line string) (zapcore.Level, string) {
	line = strings.TrimSpace(line)

	tag, msg, ok := strings.Cut(line, " ")
	if !ok {
		return zapcore.InfoLevel, line
	}

	switch tag {
	case "[TRACE]", "[DEBUG]":
		return zapcore.DebugLevel, msg
	case "[INFO]":
		return zapcore.InfoLevel, msg
	case "[WARN]":
		return zapcore.WarnLevel, msg
	case "[ERR]", "[ERROR]":
		return zapcore.ErrorLevel, msg
	default:
		return zapcore.InfoLevel, line
	}
}
