package pbm

import (
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Logger struct {
	cn   *PBM      // pbm object
	out  io.Writer // duplicate log to
	rs   string
	node string
}

type LogEntry struct {
	TS      int64     `bson:"ts" json:"ts"`
	RS      string    `bson:"rs" json:"rs"`
	Node    string    `bson:"node" json:"node"`
	Type    EntryType `bson:"type" json:"type"`
	Action  Command   `bson:"action" json:"action"`
	ObjName string    `bson:"obj" json:"obj"`
	Msg     string    `bson:"msg" json:"msg"`
}

const logTimeFormat = "2006/01/02 15:04:05"

func (e *LogEntry) formatTS() string {
	return time.Unix(e.TS, 0).UTC().Format(logTimeFormat)
}

func (e *LogEntry) String() (s string) {
	if e.Action != CmdUndefined || e.ObjName != "" {
		id := []string{}
		if e.Action != CmdUndefined {
			id = append(id, string(e.Action))
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

func NewLogger(cn *PBM, rs, node string) *Logger {
	return &Logger{
		cn:   cn,
		rs:   rs,
		node: node,
	}
}

// SetOut set additional output for the logs
func (l *Logger) SetOut(w io.Writer) {
	l.out = w
}

func (l *Logger) output(typ EntryType, action Command, obj, msg string, args ...interface{}) {
	if len(args) > 0 {
		msg = fmt.Sprintf(msg, args...)
	}
	e := &LogEntry{
		TS:      time.Now().UTC().Unix(),
		RS:      l.rs,
		Node:    l.node,
		Type:    typ,
		Action:  action,
		ObjName: obj,
		Msg:     msg,
	}

	err := l.Output(e)
	if err != nil {
		log.Printf("[ERROR] wrting log: %v, entry: %s", err, e)
	}
}

func (l *Logger) Info(action Command, obj, msg string, args ...interface{}) {
	l.output(TypeInfo, action, obj, msg, args...)
}

func (l *Logger) Warning(action Command, obj, msg string, args ...interface{}) {
	l.output(TypeWarning, action, obj, msg, args...)
}

func (l *Logger) Error(action Command, obj, msg string, args ...interface{}) {
	l.output(TypeError, action, obj, msg, args...)
}

func (l *Logger) Output(e *LogEntry) error {
	var rerr error

	if l.cn != nil {
		_, err := l.cn.Conn.Database(DB).Collection(LogCollection).InsertOne(l.cn.ctx, e)
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

// LogGet returns last log entries
func (p *PBM) LogGet(rs string, typ EntryType, action Command, limit int64) ([]LogEntry, error) {
	cur, err := p.Conn.Database(DB).Collection(LogCollection).Find(
		p.ctx,
		bson.D{{"rs", rs}, {"type", typ}, {"action", action}},
		options.Find().SetLimit(limit).SetSort(bson.D{{"ts", -1}}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "get list from mongo")
	}

	logs := []LogEntry{}
	for cur.Next(p.ctx) {
		l := LogEntry{}
		err := cur.Decode(&l)
		if err != nil {
			return nil, errors.Wrap(err, "message decode")
		}
		logs = append(logs, l)
	}

	return logs, nil
}
