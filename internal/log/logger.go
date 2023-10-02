package log

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/internal/errors"
)

type loggerImpl struct {
	cn   *mongo.Collection
	out  io.Writer
	rs   string
	node string

	buf    Buffer
	bufSet atomic.Uint32

	pauseMgo int32
}

func New(cn *mongo.Collection, rs, node string) Logger {
	return &loggerImpl{
		cn:   cn,
		out:  os.Stderr,
		rs:   rs,
		node: node,
	}
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
		log.Printf("[ERROR] wrting log: %v, entry: %s", err, e)
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

	if l.cn != nil && atomic.LoadInt32(&l.pauseMgo) == 0 {
		_, err := l.cn.InsertOne(ctx, e)
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
