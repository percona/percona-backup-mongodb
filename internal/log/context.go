package log

import "github.com/percona/percona-backup-mongodb/internal/context"

const (
	logEventTag = "pbm:log:event"
	loggerTag   = "pbm:logger"
)

func GetLogEventFromContextOr(ctx context.Context, fallback *Event) *Event {
	val := ctx.Value(logEventTag)
	if val == nil {
		return fallback
	}

	ev, ok := val.(*Event)
	if !ok {
		return fallback
	}

	return ev
}

func SetLogEventToContext(ctx context.Context, ev *Event) context.Context {
	return context.WithValue(ctx, logEventTag, ev)
}

func GetLoggerFromContextOr(ctx context.Context, fallback *Logger) *Logger {
	val := ctx.Value(loggerTag)
	if val == nil {
		return fallback
	}

	ev, ok := val.(*Logger)
	if !ok {
		return fallback
	}

	return ev
}

func SetLoggerToContext(ctx context.Context, ev *Logger) context.Context {
	return context.WithValue(ctx, logEventTag, ev)
}
