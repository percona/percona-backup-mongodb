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
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

// globalLogger is the only logger used in PBM Agent, PBM CLI and PBM SDK.
var globalLogger = &loggerImpl{
	file: &FileHandler{
		out:   noCloseWriter{os.Stderr},
		level: DebugLevel,
		json:  false,
	},
	alt: discardHandler{},
}

// LogTimeFormat is a date-time format to be displayed in the log output
const LogTimeFormat = "2006-01-02T15:04:05.000-0700"

type tsFormatFn func(ts int64) string

type Level int

const (
	ErrorLevel Level = 1 + iota
	WarningLevel
	InfoLevel
	DebugLevel
)

type StringLevel string

const (
	StringErrorLevel   StringLevel = "error"
	StringWarningLevel StringLevel = "warning"
	StringInfoLevel    StringLevel = "info"
	StringDebugLevel   StringLevel = "debug"
)

func (l StringLevel) Level() Level {
	return ParseLevel(string(l))
}

// ParseLevel convert any string to Level.
// If the value is unsupported, it returns -1.
func ParseLevel(level string) Level {
	switch level {
	case string(StringDebugLevel):
		return DebugLevel
	case string(StringInfoLevel):
		return InfoLevel
	case string(StringWarningLevel):
		return WarningLevel
	case string(StringErrorLevel):
		return ErrorLevel
	default:
		return -1
	}
}

// Enabled returns true if provided level is higher or equals to configured.
//
// For example, configured ParseLevel("info").Enabled(ParseLevel("debug"))
func (l Level) Enabled(level Level) bool {
	return level <= l
}

// LongString returns StringLevel (as string).
// If the level is unsupported, return empty string.
func (l Level) LongString() string {
	switch l {
	case DebugLevel:
		return string(StringDebugLevel)
	case InfoLevel:
		return string(StringInfoLevel)
	case WarningLevel:
		return string(StringWarningLevel)
	case ErrorLevel:
		return string(StringErrorLevel)
	default:
		return ""
	}
}

// String returns short string to identify level. Used in text/formatted output.
// If the level is unsupported, return empty string.
func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "D"
	case InfoLevel:
		return "I"
	case WarningLevel:
		return "W"
	case ErrorLevel:
		return "E"
	default:
		return ""
	}
}

type ctxTag string

const attrsTag ctxTag = "pbm:attrs"

// Attrs contains context values used in PBM to identify internal processes.
type Attrs struct {
	Event string
	OPID  string
	Name  string
	Epoch primitive.Timestamp
}

var zeroAttr = Attrs{} // preallocate

// GetAttrs extracts attributes from context or nil.
func GetAttrs(ctx context.Context) *Attrs {
	attrs, _ := ctx.Value(attrsTag).(*Attrs)
	return attrs
}

// Context returns a child context with provided attributes.
func Context(ctx context.Context, opts ...AttrOption) context.Context {
	if len(opts) == 0 {
		return ctx
	}

	attrs := GetAttrs(ctx)
	if attrs == nil {
		attrs = &Attrs{}
	}
	for _, apply := range opts {
		apply(attrs)
	}
	return context.WithValue(ctx, attrsTag, attrs)
}

// Copy copies only attributes from one context to another.
//
// Useful when these contexts should be decoupled.
// For example, `to` and `from` do not share other context values or cancelation.
func Copy(to, from context.Context) context.Context {
	attrs := GetAttrs(from)
	if attrs == nil {
		return to
	}
	return context.WithValue(to, attrsTag, attrs)
}

type AttrOption func(*Attrs)

// WithAttrs overwrites event/command name.
func WithAttrs(attrs *Attrs) AttrOption {
	return func(a *Attrs) {
		if attrs == nil {
			return
		}

		a.Event = attrs.Event
		a.OPID = attrs.OPID
		a.Name = attrs.Name
		a.Epoch = attrs.Epoch
	}
}

// WithEvent overwrites event/command name.
func WithEvent(event string) AttrOption {
	return func(a *Attrs) {
		a.Event = event
	}
}

// WithEvent overwrites OPID (operation/command _id).
func WithOPID(opid string) AttrOption {
	return func(a *Attrs) {
		a.OPID = opid
	}
}

// WithEvent overwrites name value.
func WithName(name string) AttrOption {
	return func(a *Attrs) {
		a.Name = name
	}
}

// WithEvent overwrites epoch value.
func WithEpoch(epoch primitive.Timestamp) AttrOption {
	return func(a *Attrs) {
		a.Epoch = epoch
	}
}

// SetLocalHandler replaces local (file) handler. Returns previous handler.
func SetLocalHandler(h *FileHandler) Handler {
	return globalLogger.SetLocal(h)
}

// SetRemoteHandler replaces remote (e.g. MongoDB or storage.Storage) handler.
// Returns previous handler.
func SetRemoteHandler(h Handler) Handler {
	return globalLogger.SetRemote(h)
}

// Debug logs with "debug" level. Attributes are extracting from context.
func Debug(ctx context.Context, msg string, args ...any) {
	globalLogger.LogContext(ctx, DebugLevel, msg, args...)
}

// Info logs with "info" level. Attributes are extracting from context.
func Info(ctx context.Context, msg string, args ...any) {
	globalLogger.LogContext(ctx, InfoLevel, msg, args...)
}

// Warn logs with "warning" level. Attributes are extracting from context.
func Warn(ctx context.Context, msg string, args ...any) {
	globalLogger.LogContext(ctx, WarningLevel, msg, args...)
}

// Error logs with "error" level. Attributes are extracting from context.
func Error(ctx context.Context, msg string, args ...any) {
	globalLogger.LogContext(ctx, ErrorLevel, msg, args...)
}

// DebugAttrs logs with "debug" level and attributes.
func DebugAttrs(attrs *Attrs, msg string, args ...any) {
	globalLogger.LogAttrs(attrs, DebugLevel, msg, args...)
}

// InfoAttrs logs with "info" level and attributes.
func InfoAttrs(attrs *Attrs, msg string, args ...any) {
	globalLogger.LogAttrs(attrs, InfoLevel, msg, args...)
}

// WarnAttrs logs with "warning" level and attributes.
func WarnAttrs(attrs *Attrs, msg string, args ...any) {
	globalLogger.LogAttrs(attrs, WarningLevel, msg, args...)
}

// ErrorAttrs logs with "error" level and attributes.
func ErrorAttrs(attrs *Attrs, msg string, args ...any) {
	globalLogger.LogAttrs(attrs, ErrorLevel, msg, args...)
}

// Handler is write/format layer used by Logger.
// Within PBM, it allows to use any custom handler (like write to S3)
// without modifying logger.
type Handler interface {
	// Log does format/encoding for log.Record.
	Log(r *Record) error

	Close() error
}

// discardHandler logs nothing. Used as placeholder.
type discardHandler struct{}

func (discardHandler) Log(*Record) error { return nil }
func (discardHandler) Close() error      { return nil }

// FileHandler writes to local file logs as JSON or formated text.
// See Record.String() for the text format.
type FileHandler struct {
	out   io.WriteCloser
	level Level
	json  bool
}

var _ Handler = &FileHandler{}

// NewFileHandler open or creates file as write-only and append-only with 0666 umask.
// It creates all dirs in the path for the file with 0777 umask.
// If "/dev/stderr" is provided, it will uses os.Stderr (on Close the os.Stderr keeps to be open).
func NewFileHandler(path string, level Level, json bool) (*FileHandler, error) {
	if path == "/dev/stderr" {
		return &FileHandler{out: noCloseWriter{os.Stderr}, level: level, json: json}, nil
	}

	out, err := newLogFile(path)
	if err != nil {
		return nil, errors.Wrap(err, "open file")
	}

	return &FileHandler{out: out, level: level, json: json}, nil
}

func newLogFile(filename string) (*os.File, error) {
	fullpath, err := filepath.Abs(filename)
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

	return os.OpenFile(fullpath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
}

func (h *FileHandler) Log(r *Record) error {
	if !h.level.Enabled(r.Level) {
		return nil
	}

	if h.json {
		return json.NewEncoder(h.out).Encode(r)
	}

	_, err := h.out.Write([]byte(r.String() + "\n"))
	return err
}

func (h *FileHandler) Close() error {
	var err0 error
	if f, ok := h.out.(*os.File); ok {
		err0 = f.Sync()
	}
	err1 := h.out.Close()
	return errors.Join(err0, err1)
}

// mongoHandler default PBM log handler for centralized loggining.
type mongoHandler struct {
	coll *mongo.Collection
}

func NewMongoHandler(m *mongo.Client) *mongoHandler {
	return &mongoHandler{coll: m.Database(defs.DB).Collection(defs.LogCollection)}
}

func (h *mongoHandler) Log(r *Record) error {
	_, err := h.coll.InsertOne(context.Background(), r)
	return err
}

func (h *mongoHandler) Close() error {
	return nil
}

type noCloseWriter struct{ io.Writer }

func (w noCloseWriter) Write(p []byte) (int, error) {
	return w.Writer.Write(p)
}

func (w noCloseWriter) Close() error {
	return nil
}

type loggerImpl struct {
	mu sync.Mutex

	// file is local log output to file. (by default /dev/stderr)
	file *FileHandler

	// default PBM (remote) log output.
	// nil if `alt` is set.
	mongo *mongoHandler

	// alt is any custom handler (but never non mongoHandler).
	// nil if `mongo` is set.
	alt Handler
}

// New creates new logger with provided handlers.
func New(local *FileHandler, remote Handler) *loggerImpl {
	l := &loggerImpl{}
	l.SetLocal(local)
	l.SetRemote(remote)
	return l
}

// SetLocal sets local/file log handler.
// `nil` values sets default handler (file: stderr, level: debug, format: text).
func (l *loggerImpl) SetLocal(h *FileHandler) Handler {
	l.mu.Lock()
	defer l.mu.Unlock()

	if h == nil {
		h = &FileHandler{
			out:   noCloseWriter{os.Stderr},
			level: DebugLevel,
			json:  false,
		}
	}

	r := l.file
	l.file = h
	return r
}

// SetRemote sets remote log handler.
// `nil` value disables remote handler.
func (l *loggerImpl) SetRemote(h Handler) Handler {
	l.mu.Lock()
	defer l.mu.Unlock()

	if h == nil {
		h = discardHandler{}
	}

	var rv Handler
	if l.mongo != nil {
		rv = l.mongo
		l.mongo = nil
	} else {
		rv = l.alt
		l.alt = nil
	}

	if m, ok := h.(*mongoHandler); ok {
		l.mongo = m
	} else {
		l.alt = h
	}

	return rv
}

func (l *loggerImpl) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.mongo.Close()
	l.file.Close()
}

// LogContext logs messages with provided level.
// Attributes are extracting from context.
func (l *loggerImpl) LogContext(ctx context.Context, level Level, msg string, args ...any) {
	attrs := GetAttrs(ctx)
	if attrs == nil {
		attrs = &zeroAttr
	}

	l.LogAttrs(attrs, level, msg, args...)
}

// LogContext logs messages with provided level and attributes.
func (l *loggerImpl) LogAttrs(attrs *Attrs, level Level, msg string, args ...any) {
	if attrs == nil {
		attrs = &zeroAttr
	}
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	//nolint:gosmopolitan
	_, tz := time.Now().Local().Zone()
	t := time.Now().UTC()

	r := &Record{
		Msg: msg,
		RecordAttrs: RecordAttrs{
			RS:      defs.Replset(),
			Node:    defs.NodeID(),
			Level:   level,
			Event:   attrs.Event,
			ObjName: attrs.Name,
			OPID:    attrs.OPID,
			Epoch:   attrs.Epoch,
		},
		TS:    t.Unix(),
		Tns:   t.Nanosecond(),
		TZone: tz,
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	l.logImpl(r)
}

func (l *loggerImpl) logImpl(r *Record) {
	err := l.file.Log(r)
	if err != nil {
		log.Printf("ERROR: writing to local: %v, entry: %s\n", err, r)
	}

	if l.mongo != nil {
		err = l.mongo.Log(r)
	} else {
		err = l.alt.Log(r)
	}
	if err != nil {
		log.Printf("ERROR: sending log to remote: %v, entry: %s\n", err, r)
	}
}
