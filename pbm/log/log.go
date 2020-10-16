package log

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Logger struct {
	cn   *mongo.Collection
	out  io.Writer
	rs   string
	node string
}

type LogEntry struct {
	Time    string `bson:"-" json:"t"`
	TS      int64  `bson:"ts" json:"-"`
	Tns     int    `bson:"ns" json:"-"`
	TZone   int    `bson:"tz" json:"-"`
	LogKeys `bson:",inline" json:",inline"`
	Msg     string `bson:"msg" json:"msg"`
}

type LogKeys struct {
	Severity Severity `bson:"s" json:"s"`
	RS       string   `bson:"rs" json:"rs"`
	Node     string   `bson:"node" json:"node"`
	Event    string   `bson:"e" json:"e"`
	ObjName  string   `bson:"obj" json:"obj"`
}

// LogTimeFormat is a date-time format to be displayed in the log output
const LogTimeFormat = "2006-01-02T15:04:05.000-0700"

func (e *LogEntry) formatTS() string {
	return time.Unix(e.TS, 0).Local().Format(LogTimeFormat)
}

func (e *LogEntry) String() (s string) {
	return e.string(false)
}

func (e *LogEntry) StringNode() (s string) {
	return e.string(true)
}

func (e *LogEntry) string(showNode bool) (s string) {
	node := ""
	if showNode {
		node = " [" + e.RS + "/" + e.Node + "]"
	}

	if e.Event != "" || e.ObjName != "" {
		id := []string{}
		if e.Event != "" {
			id = append(id, string(e.Event))
		}
		if e.ObjName != "" {
			id = append(id, e.ObjName)
		}
		s = fmt.Sprintf("%s %s%s [%s] %s", e.formatTS(), e.Severity, node, strings.Join(id, "/"), e.Msg)
	} else {
		s = fmt.Sprintf("%s %s%s %s", e.formatTS(), e.Severity, node, e.Msg)
	}

	return s
}

type Severity int

const (
	Fatal Severity = iota
	Error
	Warning
	Info
	Debug
)

func (s Severity) String() string {
	switch s {
	case Fatal:
		return "F"
	case Error:
		return "E"
	case Warning:
		return "W"
	case Info:
		return "I"
	case Debug:
		return "D"
	default:
		return ""
	}
}

func New(cn *mongo.Collection, rs, node string) *Logger {
	return &Logger{
		cn:   cn,
		out:  os.Stderr,
		rs:   rs,
		node: node,
	}
}

// SetOut set io output for the logs
func (l *Logger) SetOut(w io.Writer) {
	l.out = w
}

func (l *Logger) output(s Severity, event string, obj, msg string, args ...interface{}) {
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	_, tz := time.Now().Local().Zone()
	t := time.Now().UTC()

	e := &LogEntry{
		TS:    t.Unix(),
		Tns:   t.Nanosecond(),
		TZone: tz,
		LogKeys: LogKeys{
			RS:       l.rs,
			Node:     l.node,
			Severity: s,
			Event:    event,
			ObjName:  obj,
		},
		Msg: msg,
	}

	err := l.Output(e)
	if err != nil {
		log.Printf("[ERROR] wrting log: %v, entry: %s", err, e)
	}
}

func (l *Logger) Printf(msg string, args ...interface{}) {
	l.output(Info, "", "", msg, args...)
}

func (l *Logger) Info(event string, obj, msg string, args ...interface{}) {
	l.output(Info, event, obj, msg, args...)
}

func (l *Logger) Warning(event string, obj, msg string, args ...interface{}) {
	l.output(Warning, event, obj, msg, args...)
}

func (l *Logger) Error(event string, obj, msg string, args ...interface{}) {
	l.output(Error, event, obj, msg, args...)
}

func (l *Logger) Output(e *LogEntry) error {
	var rerr error

	if l.cn != nil {
		_, err := l.cn.InsertOne(context.TODO(), e)
		if err != nil {
			rerr = errors.Wrap(err, "db")
		}
	}

	if l.out != nil {
		_, err := l.out.Write(append([]byte(e.String()), '\n'))

		err = errors.Wrap(err, "io")
		if rerr != nil {
			rerr = errors.Errorf("%v, %v", rerr, err)
		} else {
			rerr = err
		}
	}

	return rerr
}

// Event provides logging for some event (backup, restore)
type Event struct {
	l   *Logger
	typ string
	obj string
}

func (l *Logger) NewEvent(typ, name string) *Event {
	return &Event{
		l:   l,
		typ: typ,
		obj: name,
	}
}

func (e *Event) Info(msg string, args ...interface{}) {
	e.l.Info(e.typ, e.obj, msg, args...)
}

func (e *Event) Warning(msg string, args ...interface{}) {
	e.l.Warning(e.typ, e.obj, msg, args...)
}

func (e *Event) Error(msg string, args ...interface{}) {
	e.l.Error(e.typ, e.obj, msg, args...)
}

type LogRequest struct {
	TimeMin time.Time
	TimeMax time.Time
	LogKeys
}

func Get(cn *mongo.Collection, r *LogRequest, limit int64) ([]LogEntry, error) {
	filter := bson.D{bson.E{"s", bson.M{"$lte": r.Severity}}}
	if r.RS != "" {
		filter = append(filter, bson.E{"rs", r.RS})
	}
	if r.Node != "" {
		filter = append(filter, bson.E{"node", r.Node})
	}
	if r.Event != "" {
		filter = append(filter, bson.E{"e", r.Event})
	}
	if r.ObjName != "" {
		filter = append(filter, bson.E{"obj", r.ObjName})
	}
	if !r.TimeMin.IsZero() {
		filter = append(filter, bson.E{"ts", bson.M{"$gte": r.TimeMin.Unix()}})
	}
	if !r.TimeMax.IsZero() {
		filter = append(filter, bson.E{"ts", bson.M{"$lte": r.TimeMax.Unix()}})
	}

	// fmt.Printf("%#v\n\n", r.LogKeys)

	cur, err := cn.Find(
		context.TODO(),
		filter,
		options.Find().SetLimit(limit).SetSort(bson.D{{"ts", -1}, {"ns", -1}}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "get list from mongo")
	}

	logs := []LogEntry{}
	for cur.Next(context.TODO()) {
		l := LogEntry{}
		err := cur.Decode(&l)
		if err != nil {
			return nil, errors.Wrap(err, "message decode")
		}
		logs = append(logs, l)
	}

	return logs, nil
}
