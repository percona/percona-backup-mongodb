package log

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

const (
	logPathStdErr = "/dev/stderr"
)

type loggerImpl struct {
	conn   connect.Client
	logger *logger
	rs     string
	node   string

	buf    Buffer
	bufSet atomic.Uint32

	pauseMgo int32

	logLevel Severity
	logJSON  bool
	logOpts  Opts // original logging options from the command line
}

// New creates default logger which outputs to stderr.
func New(conn connect.Client, rs, node string) Logger {
	return &loggerImpl{
		conn:     conn,
		logger:   newStdLogger(),
		rs:       rs,
		node:     node,
		logLevel: Debug,
	}
}

// NewWithOpts creates logger based on provided options.
// It also starts config monitor for the purpose of hot-reload of PBM's configuration.
func NewWithOpts(ctx context.Context, conn connect.Client, rs, node string, opts *Opts, logOpts *Opts) Logger {
	l := &loggerImpl{
		conn:     conn,
		rs:       rs,
		node:     node,
		logLevel: strToSeverity(opts.LogLevel),
		logJSON:  opts.LogJSON,
		logOpts:  *logOpts,
	}

	if strings.TrimSpace(opts.LogPath) == "" || opts.LogPath == logPathStdErr {
		l.logger = newStdLogger()
	} else {
		fl, err := newFileLogger(opts.LogPath)
		l.logger = fl
		if err != nil {
			l.logger = newStdLogger()
			l.Printf("[ERROR] creating file logger: %v", err)
		}
	}

	go l.cfgChangeMonitor(ctx, &cfg{Logging: &loggingCfg{opts.LogPath, opts.LogLevel, &opts.LogJSON}})

	return l
}

// Opts represents options that are specified by the user.
type Opts struct {
	LogPath  string
	LogJSON  bool
	LogLevel string
}

type logger struct {
	out     io.Writer
	logPath string
}

func newStdLogger() *logger {
	return &logger{
		out:     os.Stderr,
		logPath: logPathStdErr,
	}
}

func newFileLogger(logPath string) (*logger, error) {
	fullpath, err := filepath.Abs(logPath)
	if err != nil {
		return nil, errors.Wrap(err, "abs")
	}

	fileInfo, err := os.Stat(fullpath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, errors.Wrap(err, "stat")
		}

		dirpath := filepath.Dir(fullpath)
		err = os.MkdirAll(dirpath, fs.ModeDir|0o777)
		if err != nil {
			return nil, errors.Wrap(err, "mkdir -p")
		}
	} else if fileInfo.IsDir() {
		return nil, errors.New("path is dir")
	}

	out, err := os.OpenFile(fullpath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, errors.Wrap(err, "open file")
	}

	return &logger{
		out:     out,
		logPath: logPath,
	}, nil
}

func (l *loggerImpl) NewEvent(typ, name, opid string, epoch primitive.Timestamp) LogEvent {
	return &eventImpl{
		l:    l,
		typ:  typ,
		obj:  name,
		ep:   epoch,
		opid: opid,
	}
}

func (l *loggerImpl) NewDefaultEvent() LogEvent {
	return &eventImpl{l: l}
}

func (l *loggerImpl) SefBuffer(b Buffer) {
	l.buf = b
	l.bufSet.Store(1)
}

func (l *loggerImpl) Close() {
	if l.bufSet.Load() == 1 && l.buf != nil {
		// don't write buffer anymore. Flush() uses storage.Save() which may
		// have logging. And writing to the buffer during Flush() will cause
		// deadlock.
		l.bufSet.Store(0)
		err := l.buf.Flush()
		if err != nil {
			log.Printf("flush log buffer on Close: %v", err)
		}
	}
}

func (l *loggerImpl) PauseMgo() {
	atomic.StoreInt32(&l.pauseMgo, 1)
}

func (l *loggerImpl) ResumeMgo() {
	atomic.StoreInt32(&l.pauseMgo, 0)
}

func (l *loggerImpl) output(
	s Severity,
	event,
	obj,
	opid string,
	epoch primitive.Timestamp,
	msg string,
	args ...interface{},
) {
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	//nolint:gosmopolitan
	_, tz := time.Now().Local().Zone()
	t := time.Now().UTC()

	e := &Entry{
		TS:    t.Unix(),
		Tns:   t.Nanosecond(),
		TZone: tz,
		LogKeys: LogKeys{
			RS:       l.rs,
			Node:     l.node,
			Severity: s,
			Event:    event,
			ObjName:  obj,
			OPID:     opid,
			Epoch:    epoch,
		},
		Msg: msg,
	}

	err := l.Output(context.TODO(), e)
	if err != nil {
		log.Printf("[ERROR] writing log: %v, entry: %s", err, e)
	}
}

func (l *loggerImpl) Printf(msg string, args ...interface{}) {
	l.output(Info, "", "", "", primitive.Timestamp{}, msg, args...)
}

func (l *loggerImpl) Debug(event, obj, opid string, epoch primitive.Timestamp, msg string, args ...interface{}) {
	l.output(Debug, event, obj, opid, epoch, msg, args...)
}

func (l *loggerImpl) Info(event, obj, opid string, epoch primitive.Timestamp, msg string, args ...interface{}) {
	l.output(Info, event, obj, opid, epoch, msg, args...)
}

func (l *loggerImpl) Warning(event, obj, opid string, epoch primitive.Timestamp, msg string, args ...interface{}) {
	l.output(Warning, event, obj, opid, epoch, msg, args...)
}

func (l *loggerImpl) Error(event, obj, opid string, epoch primitive.Timestamp, msg string, args ...interface{}) {
	l.output(Error, event, obj, opid, epoch, msg, args...)
}

func (l *loggerImpl) Fatal(event, obj, opid string, epoch primitive.Timestamp, msg string, args ...interface{}) {
	l.output(Fatal, event, obj, opid, epoch, msg, args...)
}

func (l *loggerImpl) Output(ctx context.Context, e *Entry) error {
	var rerr error

	if l.conn.LogCollection() != nil && atomic.LoadInt32(&l.pauseMgo) == 0 {
		_, err := l.conn.LockCollection().InsertOne(ctx, e)
		if err != nil {
			rerr = errors.Wrap(err, "db")
		}
	}

	// once buffer is set, it's expected to remain the same
	// until the agent is alive
	if l.bufSet.Load() == 1 && l.buf != nil {
		err := json.NewEncoder(l.buf).Encode(e)

		err = errors.Wrap(err, "buf")
		if rerr != nil {
			rerr = errors.Errorf("%v, %v", rerr, err)
		} else {
			rerr = err
		}
	}

	if l.logger != nil && l.logLevel >= e.Severity {
		var err error
		if l.logJSON {
			err = json.NewEncoder(l.logger.out).Encode(e)
			err = errors.Wrap(err, "io json")
		} else {
			_, err = l.logger.out.Write(append([]byte(e.String()), '\n'))
			err = errors.Wrap(err, "io text")
		}

		if rerr != nil {
			rerr = errors.Errorf("%v, %v", rerr, err)
		} else {
			rerr = err
		}
	}

	return rerr
}

// todo: fix concurrency issues here
func (l *loggerImpl) applyConfigChange(cfg *loggingCfg) {
	optsToApply := l.mergeLogOpts(cfg)

	if l.logger.logPath != optsToApply.LogPath {
		if strings.TrimSpace(optsToApply.LogPath) == "" || optsToApply.LogPath == logPathStdErr {
			l.logger = newStdLogger()
		} else {
			fl, err := newFileLogger(optsToApply.LogPath)
			if err != nil {
				l.logger = newStdLogger()
				l.Printf("error while doing logger type reconfig: %v", err)
			} else {
				l.logger = fl
			}
		}
	}
	if l.logLevel.String() != optsToApply.LogLevel {
		l.logLevel = strToSeverity(optsToApply.LogLevel)
	}
	if l.logJSON != optsToApply.LogJSON {
		l.logJSON = optsToApply.LogJSON
	}
}

// mergeLogOpts creates new log options based on config changes.
// In case when config is not defined, options from cmd line are applied.
func (l *loggerImpl) mergeLogOpts(cfg *loggingCfg) *Opts {
	opts := l.logOpts

	if cfg == nil {
		return &opts
	}

	if cfg.Path != "" {
		opts.LogPath = cfg.Path
	}
	if cfg.Level != "" {
		opts.LogLevel = cfg.Level
	}
	if cfg.JSON != nil {
		opts.LogJSON = *cfg.JSON
	}

	return &opts
}
