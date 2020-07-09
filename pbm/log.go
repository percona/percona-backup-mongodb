package pbm

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/pkg/errors"
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

func (e *LogEntry) String() string {
	s := fmt.Sprintf("%s [%s] %s | {%s / %s}", e.formatTS(), e.Type, e.Msg)
	if e.Action != CmdUndefined || e.ObjName != "" {
		s += fmt.Sprintf(" <%s / %s>", e.Action, e.ObjName)
	}

	return s
}

type EntryType string

const (
	TypeInfo    EntryType = "info"
	TypeWarning EntryType = "warning"
	TypeError   EntryType = "error"
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

func (l *Logger) output(typ EntryType, msg string, action Command, obj string) {
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
		log.Printf("[ERROR] wrting log entry: %v, entry: %s", err, e)
	}
}

func (l *Logger) Info(msg string, action Command, obj string) {
	l.output(TypeInfo, msg, action, obj)
}

func (l *Logger) Warning(msg string, action Command, obj string) {
	l.output(TypeWarning, msg, action, obj)
}

func (l *Logger) Error(msg string, action Command, obj string) {
	l.output(TypeError, msg, action, obj)
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
