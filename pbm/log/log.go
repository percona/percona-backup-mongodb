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
	TS      int64     `bson:"ts" json:"ts"`
	TZone   int       `bson:"tz" json:"tz"`
	RS      string    `bson:"rs" json:"rs"`
	Node    string    `bson:"node" json:"node"`
	Type    EntryType `bson:"type" json:"type"`
	Event   string    `bson:"event" json:"event"`
	ObjName string    `bson:"obj" json:"obj"`
	Msg     string    `bson:"msg" json:"msg"`
}

// LogTimeFormat is a date-time format to be displayed in the log output
const LogTimeFormat = "2006-01-02T15:04:05.000-0700"

func (e *LogEntry) formatTS() string {
	return time.Unix(e.TS, 0).Local().Format(LogTimeFormat)
}

func (e *LogEntry) String() (s string) {
	if e.Event != "" || e.ObjName != "" {
		id := []string{}
		if e.Event != "" {
			id = append(id, string(e.Event))
		}
		if e.ObjName != "" {
			id = append(id, e.ObjName)
		}
		s = fmt.Sprintf("%s [%s] %s: %s", e.formatTS(), e.Type, strings.Join(id, "/"), e.Msg)
	} else {
		s = fmt.Sprintf("%s [%s] %s", e.formatTS(), e.Type, e.Msg)
	}

	return s
}

type EntryType string

const (
	TypeInfo    EntryType = "INFO"
	TypeWarning EntryType = "Warning"
	TypeError   EntryType = "ERROR"
)

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

func (l *Logger) output(typ EntryType, event string, obj, msg string, args ...interface{}) {
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	_, tz := time.Now().Local().Zone()
	e := &LogEntry{
		TS:      time.Now().UTC().Unix(),
		TZone:   tz,
		RS:      l.rs,
		Node:    l.node,
		Type:    typ,
		Event:   event,
		ObjName: obj,
		Msg:     msg,
	}

	err := l.Output(e)
	if err != nil {
		log.Printf("[ERROR] wrting log: %v, entry: %s", err, e)
	}
}

func (l *Logger) Printf(msg string, args ...interface{}) {
	l.output(TypeInfo, "", "", msg, args...)
}

func (l *Logger) Info(event string, obj, msg string, args ...interface{}) {
	l.output(TypeInfo, event, obj, msg, args...)
}

func (l *Logger) Warning(event string, obj, msg string, args ...interface{}) {
	l.output(TypeWarning, event, obj, msg, args...)
}

func (l *Logger) Error(event string, obj, msg string, args ...interface{}) {
	l.output(TypeError, event, obj, msg, args...)
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
	e.l.Info(e.typ, e.obj, msg, args)
}

func (e *Event) Warning(msg string, args ...interface{}) {
	e.l.Warning(e.typ, e.obj, msg, args)
}

func (e *Event) Error(msg string, args ...interface{}) {
	e.l.Error(e.typ, e.obj, msg, args)
}

// GetEntries returns last log entries
func GetEntries(cn *mongo.Collection, rs string, typ EntryType, event string, limit int64) ([]LogEntry, error) {
	// TODO: it should be reworked along with the status implementation
	// TODO: add indexes, make logs retrieval more flexible
	filter := bson.D{{"type", typ}, {"action", event}}
	if rs != "" {
		filter = append(filter, bson.E{"rs", rs})
	}

	cur, err := cn.Find(
		context.TODO(),
		filter,
		options.Find().SetLimit(limit).SetSort(bson.D{{"ts", -1}}),
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
