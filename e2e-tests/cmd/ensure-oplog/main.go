package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/pitr"
)

var logger = log.New(os.Stdout, "", log.LstdFlags)

func main() {
	ctx := context.Background()

	app := kingpin.New("ensure-oplog", "ensure oplog chunks")
	cmd := app.Command("run", "").Default().Hidden()
	uri := cmd.Flag("mongodb-uri", "mongodb URI").Envar("PBM_MONGODB_URI").String()
	fromS := cmd.Flag("from", "first op time").String()
	tillS := cmd.Flag("till", "last op time").String()
	_, err := app.DefaultEnvars().Parse(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	fromTS, err := parseTS(*fromS)
	if err != nil {
		log.Fatalf("parse from time: %s", err.Error())
	}
	tillTS, err := parseTS(*tillS)
	if err != nil {
		log.Fatalf("parse till time: %s", err.Error())
	}

	t, err := connTopo(ctx, *uri)
	if err != nil {
		log.Fatalf("getTopo: %s", err.Error())
	}

	switch t {
	case topoMongos:
		err = ensureClusterOplog(ctx, *uri, fromTS, tillTS)
	case topoReplset:
		err = ensureReplsetOplog(ctx, *uri, fromTS, tillTS)
	default:
		err = errors.New("unsupported connection")
	}

	if err != nil {
		log.Fatal(err)
	}
}

type topo int

const (
	topoUnknown = topo(iota)
	topoMongos
	topoReplset
)

func connTopo(ctx context.Context, uri string) (topo, error) {
	m, err := connect(ctx, uri)
	if err != nil {
		return topoUnknown, errors.WithMessage(err, "connect")
	}
	defer m.Disconnect(context.Background()) // nolint:errcheck

	r, err := sayHello(ctx, m)
	if err != nil {
		return topoUnknown, errors.WithMessage(err, "getShortHello")
	}

	switch {
	case r.Msg == "isdbgrid":
		return topoMongos, nil
	case r.SetName != "":
		return topoReplset, nil
	}

	return topoUnknown, nil
}

func parseTS(t string) (ts primitive.Timestamp, err error) {
	if len(t) == 0 {
		return
	}

	if tt, ii, ok := strings.Cut(t, ","); ok {
		t, err := strconv.ParseUint(tt, 10, 32)
		if err != nil {
			return ts, errors.WithMessage(err, "parse clusterTime T")
		}
		i, err := strconv.ParseUint(ii, 10, 32)
		if err != nil {
			return ts, errors.WithMessage(err, "parse clusterTime I")
		}

		ts.T = uint32(t)
		ts.I = uint32(i)
		return ts, nil
	}

	const datetimeFormat = "2006-01-02T15:04:05"
	const dateFormat = "2006-01-02"

	var tsto time.Time
	switch len(t) {
	case len(datetimeFormat):
		tsto, err = time.Parse(datetimeFormat, t)
	case len(dateFormat):
		tsto, err = time.Parse(dateFormat, t)
	default:
		err = errors.New("invalid format")
	}

	if err != nil {
		return ts, errors.WithMessage(err, "parse date")
	}

	ts.T = uint32(tsto.Unix())
	return ts, nil
}

func connect(ctx context.Context, uri string) (*mongo.Client, error) {
	if !strings.HasPrefix(uri, "mongodb://") {
		uri = "mongodb://" + uri
	}

	m, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, errors.WithMessage(err, "connect")
	}

	if err = m.Ping(ctx, nil); err != nil {
		m.Disconnect(context.Background()) // nolint:errcheck
		return nil, errors.WithMessage(err, "ping")
	}

	return m, nil
}

type hello struct {
	Msg     string
	SetName string
}

func sayHello(ctx context.Context, m *mongo.Client) (*hello, error) {
	res := m.Database("admin").RunCommand(ctx, bson.D{{"hello", 1}})
	if err := res.Err(); err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	var r *hello
	err := res.Decode(&r)
	return r, errors.WithMessage(err, "decode")
}

func ensureClusterOplog(ctx context.Context, uri string, from, till primitive.Timestamp) error {
	logger.Printf("[%s] ensuring cluster oplog: %s - %s",
		uri, formatTimestamp(from), formatTimestamp(from))

	m, err := connect(ctx, uri)
	if err != nil {
		return errors.WithMessage(err, "connect")
	}
	defer m.Disconnect(context.Background()) // nolint:errcheck

	res := m.Database("admin").RunCommand(ctx, bson.D{{"getShardMap", 1}})
	if err := res.Err(); err != nil {
		return errors.WithMessage(err, "getShardMap: query")
	}

	var r struct{ ConnStrings map[string]string }
	if err := res.Decode(&r); err != nil {
		return errors.WithMessage(err, "getShardMap: decode")
	}

	eg, gc := errgroup.WithContext(ctx)
	for hosts := range r.ConnStrings {
		id, rsURI, _ := strings.Cut(hosts, "/")

		eg.Go(func() error {
			err := ensureReplsetOplog(gc, rsURI, from, till)
			return errors.WithMessagef(err, "[%s] ensure oplog", id)
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	logger.Printf("[%s] ensured cluster oplog: %s - %s",
		uri, formatTimestamp(from), formatTimestamp(from))

	return nil
}

func ensureReplsetOplog(ctx context.Context, uri string, from, till primitive.Timestamp) error {
	logger.Printf("[%s] ensure replset oplog: %s - %s",
		uri, formatTimestamp(from), formatTimestamp(from))

	m, err := connect(ctx, uri)
	if err != nil {
		return errors.WithMessage(err, "connect")
	}

	info, err := sayHello(ctx, m)
	if err != nil {
		return errors.WithMessage(err, "get node info")
	}
	if info.SetName == "" {
		return errors.New("cannot ensure oplog in standalone mode")
	}

	firstOpT, err := findPreviousOplogTS(ctx, m, from)
	if err != nil {
		return errors.WithMessage(err, "lookup first oplog record")
	}

	lastOpT, err := findFollowingOplogTS(ctx, m, till)
	if err != nil {
		return errors.WithMessage(err, "lookup first oplog record")
	}

	logger.Printf("[%s] ensuring replset oplog (actual): %s - %s",
		uri, formatTimestamp(firstOpT), formatTimestamp(lastOpT))

	pbmC, err := pbm.New(ctx, uri, "ensure-oplog")
	if err != nil {
		return errors.WithMessage(err, "connect to PBM")
	}

	chunks, err := pbmC.PITRGetChunksSlice(info.SetName, firstOpT, lastOpT)
	if err != nil {
		return errors.WithMessage(err, "get chunks")
	}

	missedChunks := findChunkRanges(chunks, firstOpT, lastOpT)
	if len(missedChunks) == 0 {
		return errors.New("no missed oplog chunk")
	}

	cfg, err := pbmC.GetConfig()
	if err != nil {
		return errors.WithMessage(err, "get config")
	}

	stg, err := pbmC.GetStorage(nil)
	if err != nil {
		return errors.WithMessage(err, "get storage")
	}

	compression := compress.CompressionType(cfg.PITR.Compression)

	for _, t := range missedChunks {
		logger.Printf("[%s] ensure missed chunk: %s - %s",
			uri, formatTimestamp(t.from), formatTimestamp(t.till))

		filename := pitr.ChunkName(info.SetName, t.from, t.till, compression)
		o := oplog.NewOplogBackup(m)
		o.SetTailingSpan(t.from, t.till)

		_, err = backup.Upload(ctx, o, stg, compression, cfg.PITR.CompressionLevel, filename, -1)
		if err != nil {
			return errors.WithMessagef(err, "failed to upload %s - %s chunk",
				formatTimestamp(t.from), formatTimestamp(t.till))
		}

		logger.Printf("[%s] uploaded chunk: %s - %s",
			uri, formatTimestamp(t.from), formatTimestamp(t.till))

		meta := pbm.OplogChunk{
			RS:          info.SetName,
			FName:       filename,
			Compression: compression,
			StartTS:     t.from,
			EndTS:       t.till,
		}

		if err := pbmC.PITRAddChunk(meta); err != nil {
			return errors.WithMessagef(err, "failed to save %s - %s chunk meta",
				formatTimestamp(t.from), formatTimestamp(t.till))
		}

		logger.Printf("[%s] saved chunk meta: %s - %s",
			uri, formatTimestamp(t.from), formatTimestamp(t.till))
	}

	logger.Printf("[%s] ensured replset oplog: %s - %s",
		uri, formatTimestamp(firstOpT), formatTimestamp(lastOpT))

	return nil
}

func findPreviousOplogTS(ctx context.Context, m *mongo.Client, ts primitive.Timestamp) (primitive.Timestamp, error) {
	f := bson.M{}
	if !ts.IsZero() {
		f["ts"] = bson.M{"$lte": ts}
	}
	o := options.FindOne().SetSort(bson.D{{"ts", -1}})
	res := m.Database("local").Collection("oplog.rs").FindOne(ctx, f, o)
	return findOplogTSHelper(res)
}

func findFollowingOplogTS(ctx context.Context, m *mongo.Client, ts primitive.Timestamp) (primitive.Timestamp, error) {
	f := bson.M{}
	if !ts.IsZero() {
		f["ts"] = bson.M{"$gte": ts}
	}
	o := options.FindOne().SetSort(bson.D{{"ts", 1}})
	res := m.Database("local").Collection("oplog.rs").FindOne(ctx, f, o)
	return findOplogTSHelper(res)
}

func findOplogTSHelper(res *mongo.SingleResult) (primitive.Timestamp, error) {
	if err := res.Err(); err != nil {
		return primitive.Timestamp{}, err
	}

	var v struct{ TS primitive.Timestamp }
	if err := res.Decode(&v); err != nil {
		return primitive.Timestamp{}, errors.WithMessage(err, "decode")
	}

	return v.TS, nil
}

type timerange struct {
	from, till primitive.Timestamp
}

func findChunkRanges(rs []pbm.OplogChunk, from, till primitive.Timestamp) []timerange {
	if len(rs) == 0 {
		return []timerange{{from, till}}
	}

	rv := []timerange{}

	c := rs[0]
	if primitive.CompareTimestamp(from, c.StartTS) == -1 {
		rv = append(rv, timerange{from, c.StartTS})
	}

	endTS := c.EndTS
	for _, c = range rs[1:] {
		if primitive.CompareTimestamp(endTS, c.StartTS) == -1 {
			rv = append(rv, timerange{endTS, c.StartTS})
		}
		if primitive.CompareTimestamp(till, c.EndTS) != 1 {
			return rv
		}

		endTS = c.EndTS
	}

	if primitive.CompareTimestamp(endTS, till) == -1 {
		rv = append(rv, timerange{endTS, till})
	}

	return rv
}

func formatTimestamp(t primitive.Timestamp) string {
	return fmt.Sprintf("%d,%d", t.T, t.I)
}
