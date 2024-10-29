package log

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

type LogRequest struct {
	TimeMin time.Time
	TimeMax time.Time
	RecordAttrs
}

type Record struct {
	Msg         string `bson:"msg" json:"msg"`
	RecordAttrs `bson:",inline" json:",inline"`
	ObjID       primitive.ObjectID `bson:"-" json:"-"` // to get sense of mgs total ordering while reading logs
	Tns         int                `bson:"ns" json:"-"`
	TZone       int                `bson:"tz" json:"-"`
	TS          int64              `bson:"ts" json:"ts"`
}

func (r *Record) Stringify(f tsFormatFn, showNode, extr bool) string {
	node := ""
	if showNode {
		node = " [" + r.RS + "/" + r.Node + "]"
	}

	var s string
	if r.Event != "" || r.ObjName != "" {
		id := []string{}
		if r.Event != "" {
			id = append(id, string(r.Event))
		}
		if r.ObjName != "" {
			id = append(id, r.ObjName)
		}
		if extr {
			id = append(id, r.OPID)
		}
		s = fmt.Sprintf("%s %s%s [%s] %s", f(r.TS), r.Level, node, strings.Join(id, "/"), r.Msg)
	} else {
		s = fmt.Sprintf("%s %s%s %s", f(r.TS), r.Level, node, r.Msg)
	}

	return s
}

func (r *Record) String() string {
	return r.Stringify(tsLocal, false, false)
}

func (r *Record) StringNode() string {
	return r.Stringify(tsLocal, true, false)
}

func tsLocal(ts int64) string {
	//nolint:gosmopolitan
	return time.Unix(ts, 0).Local().Format(LogTimeFormat)
}

type RecordAttrs struct {
	RS      string              `bson:"rs" json:"rs"`
	Node    string              `bson:"node" json:"node"`
	Event   string              `bson:"e" json:"e"`
	ObjName string              `bson:"eobj" json:"eobj"`
	OPID    string              `bson:"opid,omitempty" json:"opid,omitempty"`
	Epoch   primitive.Timestamp `bson:"ep,omitempty" json:"ep,omitempty"`
	Level   Level               `bson:"s" json:"s"`
}

type RecordList struct {
	loc      *time.Location
	Data     []Record `json:"data"`
	ShowNode bool     `json:"-"`
	Extr     bool     `json:"-"`
}

func (r *RecordList) SetLocation(l string) error {
	var err error
	r.loc, err = time.LoadLocation(l)
	return err
}

func (r RecordList) MarshalJSON() ([]byte, error) {
	data := r.Data
	if data == nil {
		data = []Record{}
	}
	return json.Marshal(data)
}

func (r RecordList) String() string {
	if r.loc == nil {
		r.loc = time.UTC
	}

	f := func(ts int64) string {
		return time.Unix(ts, 0).In(r.loc).Format(time.RFC3339)
	}

	s := ""
	for _, entry := range r.Data {
		s += entry.Stringify(f, r.ShowNode, r.Extr) + "\n"
	}

	return s
}

func buildLogFilter(r *LogRequest, exactSeverity bool) bson.D {
	filter := bson.D{bson.E{"s", bson.M{"$lte": r.Level}}}
	if exactSeverity {
		filter = bson.D{bson.E{"s", r.Level}}
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

func fetch(
	ctx context.Context,
	m connect.Client,
	r *LogRequest,
	limit int64,
	exactSeverity bool,
) (*RecordList, error) {
	filter := buildLogFilter(r, exactSeverity)
	cur, err := m.LogCollection().Find(
		ctx,
		filter,
		options.Find().SetLimit(limit).SetSort(bson.D{{"ts", -1}, {"ns", -1}}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "get list from mongo")
	}
	defer cur.Close(ctx)

	e := &RecordList{}
	for cur.Next(ctx) {
		l := Record{}
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

func CommandLastError(ctx context.Context, cc connect.Client, cid string) (string, error) {
	filter := buildLogFilter(&LogRequest{RecordAttrs: RecordAttrs{OPID: cid, Level: ErrorLevel}}, false)
	opts := options.FindOne().
		SetSort(bson.D{{"$natural", -1}}).
		SetProjection(bson.D{{"msg", 1}})
	res := cc.LogCollection().FindOne(ctx, filter, opts)
	if err := res.Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return "", nil
		}
		return "", errors.Wrap(err, "find one")
	}

	l := Record{}
	err := res.Decode(&l)
	if err != nil {
		return "", errors.Wrap(err, "message decode")
	}

	return l.Msg, nil
}

func Follow(
	ctx context.Context,
	conn connect.Client,
	r *LogRequest,
	exactSeverity bool,
) (<-chan *Record, <-chan error) {
	filter := buildLogFilter(r, exactSeverity)
	outC, errC := make(chan *Record), make(chan error)

	go func() {
		defer close(errC)
		defer close(outC)

		opt := options.Find().SetCursorType(options.TailableAwait)

		cur, err := conn.LogCollection().Find(ctx, filter, opt)
		if err != nil {
			errC <- errors.Wrap(err, "query")
			return
		}
		defer cur.Close(context.Background())

		for cur.Next(ctx) {
			e := &Record{}
			if err := cur.Decode(e); err != nil {
				errC <- errors.Wrap(err, "decode")
				return
			}

			e.ObjID, _ = cur.Current.Lookup("_id").ObjectIDOK()
			outC <- e
		}

		if err := cur.Err(); err != nil {
			errC <- err
		}
	}()

	return outC, errC
}

func LogGet(ctx context.Context, m connect.Client, r *LogRequest, limit int64) (*RecordList, error) {
	return fetch(ctx, m, r, limit, false)
}

func LogGetExactSeverity(ctx context.Context, m connect.Client, r *LogRequest, limit int64) (*RecordList, error) {
	return fetch(ctx, m, r, limit, true)
}
