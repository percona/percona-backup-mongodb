package log

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Logger struct {
	cn   *mongo.Collection
	out  io.Writer
	rs   string
	node string

	buf    Buffer
	bufSet atomic.Uint32

	pauseMgo int32
}

type Buffer interface {
	io.Writer
	Flush() error
}

type Entry struct {
	ObjID   primitive.ObjectID `bson:"-" json:"-"` // to get sense of mgs total ordering while reading logs
	TS      int64              `bson:"ts" json:"ts"`
	Tns     int                `bson:"ns" json:"-"`
	TZone   int                `bson:"tz" json:"-"`
	LogKeys `bson:",inline" json:",inline"`
	Msg     string `bson:"msg" json:"msg"`
}

type LogKeys struct {
	Severity Severity            `bson:"s" json:"s"`
	RS       string              `bson:"rs" json:"rs"`
	Node     string              `bson:"node" json:"node"`
	Event    string              `bson:"e" json:"e"`
	ObjName  string              `bson:"eobj" json:"eobj"`
	Epoch    primitive.Timestamp `bson:"ep,omitempty" json:"ep,omitempty"`
	OPID     string              `bson:"opid,omitempty" json:"opid,omitempty"`
}

// LogTimeFormat is a date-time format to be displayed in the log output
const LogTimeFormat = "2006-01-02T15:04:05.000-0700"

func tsLocal(ts int64) string {
	//nolint:gosmopolitan
	return time.Unix(ts, 0).Local().Format(LogTimeFormat)
}

func (e *Entry) String() string {
	return e.Stringify(tsLocal, false, false)
}

func (e *Entry) StringNode() string {
	return e.Stringify(tsLocal, true, false)
}

type tsformatf func(ts int64) string

func (e *Entry) Stringify(f tsformatf, showNode, extr bool) string {
	node := ""
	if showNode {
		node = " [" + e.RS + "/" + e.Node + "]"
	}

	var s string
	if e.Event != "" || e.ObjName != "" {
		id := []string{}
		if e.Event != "" {
			id = append(id, string(e.Event))
		}
		if e.ObjName != "" {
			id = append(id, e.ObjName)
		}
		if extr {
			id = append(id, e.OPID)
		}
		s = fmt.Sprintf("%s %s%s [%s] %s", f(e.TS), e.Severity, node, strings.Join(id, "/"), e.Msg)
	} else {
		s = fmt.Sprintf("%s %s%s %s", f(e.TS), e.Severity, node, e.Msg)
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

func (l *Logger) SefBuffer(b Buffer) {
	l.buf = b
	l.bufSet.Store(1)
}

func (l *Logger) Close() {
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

func (l *Logger) PauseMgo() {
	atomic.StoreInt32(&l.pauseMgo, 1)
}

func (l *Logger) ResumeMgo() {
	atomic.StoreInt32(&l.pauseMgo, 0)
}

func (l *Logger) output(
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

	err := l.Output(e)
	if err != nil {
		log.Printf("[ERROR] wrting log: %v, entry: %s", err, e)
	}
}

func (l *Logger) Printf(msg string, args ...interface{}) {
	l.output(Info, "", "", "", primitive.Timestamp{}, msg, args...)
}

func (l *Logger) Debug(event, obj, opid string, epoch primitive.Timestamp, msg string, args ...interface{}) {
	l.output(Debug, event, obj, opid, epoch, msg, args...)
}

func (l *Logger) Info(event, obj, opid string, epoch primitive.Timestamp, msg string, args ...interface{}) {
	l.output(Info, event, obj, opid, epoch, msg, args...)
}

func (l *Logger) Warning(event, obj, opid string, epoch primitive.Timestamp, msg string, args ...interface{}) {
	l.output(Warning, event, obj, opid, epoch, msg, args...)
}

func (l *Logger) Error(event, obj, opid string, epoch primitive.Timestamp, msg string, args ...interface{}) {
	l.output(Error, event, obj, opid, epoch, msg, args...)
}

func (l *Logger) Fatal(event, obj, opid string, epoch primitive.Timestamp, msg string, args ...interface{}) {
	l.output(Fatal, event, obj, opid, epoch, msg, args...)
}

func (l *Logger) Output(e *Entry) error {
	var rerr error

	if l.cn != nil && atomic.LoadInt32(&l.pauseMgo) == 0 {
		_, err := l.cn.InsertOne(context.TODO(), e)
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
	l    *Logger
	typ  string
	obj  string
	ep   primitive.Timestamp
	opid string
}

func (l *Logger) NewEvent(typ, name, opid string, epoch primitive.Timestamp) *Event {
	return &Event{
		l:    l,
		typ:  typ,
		obj:  name,
		ep:   epoch,
		opid: opid,
	}
}

func (e *Event) Debug(msg string, args ...interface{}) {
	e.l.Debug(e.typ, e.obj, e.opid, e.ep, msg, args...)
}

func (e *Event) Info(msg string, args ...interface{}) {
	e.l.Info(e.typ, e.obj, e.opid, e.ep, msg, args...)
}

func (e *Event) Warning(msg string, args ...interface{}) {
	e.l.Warning(e.typ, e.obj, e.opid, e.ep, msg, args...)
}

func (e *Event) Error(msg string, args ...interface{}) {
	e.l.Error(e.typ, e.obj, e.opid, e.ep, msg, args...)
}

func (e *Event) Fatal(msg string, args ...interface{}) {
	e.l.Fatal(e.typ, e.obj, e.opid, e.ep, msg, args...)
}

type LogRequest struct {
	TimeMin time.Time
	TimeMax time.Time
	LogKeys
}

type Entries struct {
	Data     []Entry `json:"data"`
	ShowNode bool    `json:"-"`
	Extr     bool    `json:"-"`
	loc      *time.Location
}

func (e *Entries) SetLocation(l string) error {
	var err error
	e.loc, err = time.LoadLocation(l)
	return err
}

func (e Entries) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.Data)
}

func (e Entries) String() string {
	if e.loc == nil {
		e.loc = time.UTC
	}

	f := func(ts int64) string {
		return time.Unix(ts, 0).In(e.loc).Format(time.RFC3339)
	}

	s := ""
	for _, entry := range e.Data {
		s += entry.Stringify(f, e.ShowNode, e.Extr) + "\n"
	}

	return s
}

func buildLogFilter(r *LogRequest, exactSeverity bool) bson.D {
	filter := bson.D{bson.E{"s", bson.M{"$lte": r.Severity}}}
	if exactSeverity {
		filter = bson.D{bson.E{"s", r.Severity}}
	}

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
		filter = append(filter, bson.E{"eobj", r.ObjName})
	}
	if r.Epoch.T > 0 {
		filter = append(filter, bson.E{"ep", r.Epoch})
	}
	if r.OPID != "" {
		filter = append(filter, bson.E{"opid", r.OPID})
	}
	if !r.TimeMin.IsZero() {
		filter = append(filter, bson.E{"ts", bson.M{"$gte": r.TimeMin.Unix()}})
	}
	if !r.TimeMax.IsZero() {
		filter = append(filter, bson.E{"ts", bson.M{"$lte": r.TimeMax.Unix()}})
	}

	return filter
}

func Get(cn *mongo.Collection, r *LogRequest, limit int64, exactSeverity bool) (*Entries, error) {
	filter := buildLogFilter(r, exactSeverity)
	cur, err := cn.Find(
		context.TODO(),
		filter,
		options.Find().SetLimit(limit).SetSort(bson.D{{"ts", -1}, {"ns", -1}}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "get list from mongo")
	}
	defer cur.Close(context.TODO())

	e := &Entries{}
	for cur.Next(context.TODO()) {
		l := Entry{}
		err := cur.Decode(&l)
		if err != nil {
			return nil, errors.Wrap(err, "message decode")
		}
		if id, ok := cur.Current.Lookup("_id").ObjectIDOK(); ok {
			l.ObjID = id
		}
		e.Data = append(e.Data, l)
	}

	return e, nil
}

func Follow(
	ctx context.Context,
	coll *mongo.Collection,
	r *LogRequest,
	exactSeverity bool,
) (<-chan *Entry, <-chan error) {
	filter := buildLogFilter(r, exactSeverity)
	outC, errC := make(chan *Entry), make(chan error)

	go func() {
		defer close(errC)
		defer close(outC)

		opt := options.Find().SetCursorType(options.TailableAwait)

		cur, err := coll.Find(ctx, filter, opt)
		if err != nil {
			errC <- errors.WithMessage(err, "query")
			return
		}
		defer cur.Close(context.Background())

		for cur.Next(ctx) {
			e := &Entry{}
			if err := cur.Decode(e); err != nil {
				errC <- errors.WithMessage(err, "decode")
				return
			}

			e.ObjID, _ = cur.Current.Lookup("_id").ObjectIDOK()
			outC <- e
		}
	}()

	return outC, errC
}
