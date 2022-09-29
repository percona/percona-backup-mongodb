package sharded

import (
	"context"
	"fmt"
	"io"
	"log"
	"reflect"
	"time"

	mdb "github.com/mongodb/mongo-tools/common/db"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/compress"
)

const (
	eoDB   = "eo-db"
	eoColl = "eo-coll"
	eoKey  = "t"
)

type eoTSRange struct {
	from, till primitive.Timestamp
}

// EnsureOplog tests pbm ensure-oplog command by the following steps:
//  1. generate oplog records (1...5 time ranges)
//  2. ensure oplog for 2 and 4 ranges
//  3. validate oplog records of the whole range (1-5) by comparing oplog records
//     from files with actual value in local.oplog.rs
//  4. ensure oplog for the whole 1-5 range (filling the gaps 1, 3, 5)
//  5. do step #3 for the 1-5 range
//
// In case of sharded cluster, spread data by sharding with hashed key and
// do validation on each nodes individually.
func (c *Cluster) EnsureOplog() {
	ctx := c.ctx
	mongos := c.mongos.Conn()
	shards := make(map[string]*mongo.Client, len(c.shards))
	for n, s := range c.shards {
		shards[n] = s.Conn()
	}

	if len(shards) != 1 {
		err := ensureSharding(ctx, mongos, eoDB, eoColl, bson.D{{eoKey, "hashed"}})
		if err != nil {
			log.Fatalf("ensure sharding: %s", err.Error())
		}
	}

	defer func() {
		err := mongos.Database(eoDB).Drop(ctx)
		if err != nil {
			log.Printf("drop database %s: %s", eoDB, err.Error())
		}
	}()

	var err error
	ranges := [5]eoTSRange{}
	for i := range ranges {
		ranges[i].from, ranges[i].till, err = eoPrepare(ctx, mongos)
		if err != nil {
			log.Fatalf("prepare %d: %s", i, err.Error())
		}
		fmt.Printf("time range: %s - %s\n",
			pbm.FormatTimestamp(ranges[i].from),
			pbm.FormatTimestamp(ranges[i].till))
	}

	cfg, err := getConfig(ctx, mongos)
	if err != nil {
		log.Fatalf("get config: %s", err.Error())
	}

	ss := make([]string, 0, len(c.shards))
	for rs := range c.shards {
		ss = append(ss, rs)
	}

	for i, r := range []eoTSRange{ranges[1], ranges[3], {ranges[0].from, ranges[4].till}} {
		cs, err := doEnsureOplog(ctx, mongos, ss, r.from, r.till)
		if err != nil {
			log.Fatalf("ensure oplog cmd: %s [%d]: %s",
				pbm.FormatTimestamp(r.from)+"-"+pbm.FormatTimestamp(r.till),
				i, err.Error())
		}

		fmt.Printf("validation %d: %v\n", i, cs)
		err = eoValidate(ctx, shards, cfg, cs)
		if err != nil {
			log.Fatalf("validation: %s [%d]: %s",
				pbm.FormatTimestamp(r.from)+"-"+pbm.FormatTimestamp(r.till),
				i, err.Error())
		}
	}
}

func eoPrepare(ctx context.Context, mongos *mongo.Client) (from, till primitive.Timestamp, err error) {
	from, err = getOpTime(ctx, mongos)
	if err != nil {
		err = errors.WithMessage(err, `get "from" optime`)
		return
	}

	err = eoInsertDocs(ctx, mongos, eoDB, eoColl, eoKey)
	if err != nil {
		err = errors.WithMessage(err, "insert docs")
		return
	}

	till, err = getOpTime(ctx, mongos)
	if err != nil {
		err = errors.WithMessage(err, `get "till" optime`)
		return
	}

	return from, till, nil
}

func ensureSharding(ctx context.Context, m *mongo.Client, db, coll string, key bson.D) error {
	res := m.Database("admin").RunCommand(ctx, bson.D{{"enableSharding", db}})
	if err := res.Err(); err != nil {
		return errors.WithMessage(err, "cmd: enableSharding")
	}

	q := bson.D{{"shardCollection", db + "." + coll}, {"key", key}}
	res = m.Database("admin").RunCommand(ctx, q)
	if err := res.Err(); err != nil {
		return errors.WithMessage(err, "cmd: shardCollection")
	}

	return nil
}

func getConfig(ctx context.Context, m *mongo.Client) (*pbm.Config, error) {
	res := m.Database(pbm.DB).Collection(pbm.ConfigCollection).FindOne(ctx, bson.D{})
	if err := res.Err(); err != nil {
		return nil, errors.WithMessage(err, "find")
	}

	cfg := &pbm.Config{}
	err := res.Decode(&cfg)
	if err != nil {
		return nil, errors.WithMessage(err, "decode")
	}

	if cfg.PITR.Compression == "" {
		if cfg.Backup.Compression != "" {
			cfg.PITR.Compression = cfg.Backup.Compression
		} else {
			cfg.PITR.Compression = compress.CompressionTypeS2
		}
	}

	if cfg.PITR.CompressionLevel == nil && cfg.Backup.CompressionLevel != nil {
		cfg.PITR.CompressionLevel = cfg.Backup.CompressionLevel
	}

	return cfg, nil
}

func getOpTime(ctx context.Context, m *mongo.Client) (primitive.Timestamp, error) {
	res := m.Database(pbm.DB).RunCommand(ctx, bson.D{{"hello", 1}})
	if err := res.Err(); err != nil {
		return primitive.Timestamp{}, errors.WithMessage(err, "cmd: hello")
	}

	info := pbm.NodeInfo{}
	if err := res.Decode(&info); err != nil {
		return primitive.Timestamp{}, errors.WithMessage(err, "decode")
	}

	return *info.OperationTime, nil
}

func eoInsertDocs(
	ctx context.Context,
	m *mongo.Client,
	db, coll, key string,
) error {
	for i := 0; i != 10; i++ {
		docs := make([]interface{}, 10)
		for i := range docs {
			docs[i] = map[string]int64{key: time.Now().UnixNano()}
		}

		_, err := m.Database(db).Collection(coll).InsertMany(ctx, docs)
		if err != nil {
			return err
		}
	}

	return nil
}

func doEnsureOplog(
	ctx context.Context,
	mongos *mongo.Client,
	shards []string,
	from, till primitive.Timestamp,
) (map[string][]*pbm.OplogChunk, error) {
	cmd := pbm.Cmd{
		TS:  time.Now().UTC().Unix(),
		Cmd: pbm.CmdEnsureOplog,

		EnsureOplog: &pbm.EnsureOplogCmd{From: from, Till: till},
	}

	db := mongos.Database(pbm.DB)
	res, err := db.Collection(pbm.CmdStreamCollection).InsertOne(ctx, cmd)
	if err != nil {
		return nil, errors.WithMessage(err, "cmd: ensure oplog")
	}

	ctx0, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	coll := db.Collection(pbm.PITRChunksCollection)

	for {
		time.Sleep(2 * time.Second)

		count, err := coll.CountDocuments(ctx0, bson.M{"opid": res.InsertedID})
		if err != nil {
			return nil, errors.WithMessage(err, "count documents")
		}
		if count == 0 {
			break
		}
	}

	filter := bson.D{
		{"rs", bson.M{"$in": shards}},
		{"start_ts", bson.M{"$lte": till}},
		{"end_ts", bson.M{"$gte": from}},
	}
	cur, err := coll.Find(ctx, filter)
	if err != nil {
		return nil, errors.WithMessage(err, "find")
	}

	cs := []*pbm.OplogChunk{}
	if err := cur.All(ctx, &cs); err != nil {
		return nil, errors.WithMessage(err, "decode")
	}

	rv := make(map[string][]*pbm.OplogChunk, len(shards))
	for _, c := range cs {
		rv[c.RS] = append(rv[c.RS], c)
	}

	return rv, errors.WithMessage(err, "decode")
}

func eoValidate(ctx context.Context,
	shards map[string]*mongo.Client,
	cfg *pbm.Config,
	tsRanges map[string][]*pbm.OplogChunk,
) error {
	eg, ctx := errgroup.WithContext(ctx)
	for n, sh := range shards {
		n, sh := n, sh

		chunks := tsRanges[n]

		eg.Go(func() error {
			stg, err := pbm.Storage(*cfg, nil)
			if err != nil {
				return err
			}

			for _, c := range chunks {
				c := c

				eg.Go(func() error {
					r, err := stg.SourceReader(c.FName)
					if err != nil {
						return errors.WithMessagef(err, "no chunk found: file %q", c.FName)
					}
					defer r.Close()

					r, err = compress.Decompress(r, c.Compression)
					if err != nil {
						return errors.WithMessagef(err, "no chunk found: file %q", c.FName)
					}

					err = eoCompareRecords(ctx, sh, r, c.StartTS, c.EndTS)
					if err != nil {
						return errors.WithMessage(err, "compare records")
					}

					return nil
				})
			}

			return nil
		})
	}

	return eg.Wait()
}

func eoCompareRecords(
	ctx context.Context,
	m *mongo.Client,
	r io.ReadCloser,
	from, till primitive.Timestamp,
) error {
	coll := m.Database("local").Collection("oplog.rs")
	cur, err := coll.Find(ctx, bson.D{{"ts", bson.M{"$gte": from, "$lte": till}}})
	if err != nil {
		return errors.WithMessage(err, "find")
	}
	defer cur.Close(ctx)

	src := mdb.NewBSONSource(r)
	for cur.Next(ctx) {
		if cur.Current.Lookup("op").String() == string(pbm.OperationNoop) {
			continue
		}

		doc := bson.Raw(src.LoadNext())
		if doc == nil {
			if err := src.Err(); err != nil {
				return errors.WithMessage(err, "load next bson")
			}

			return errors.WithMessage(err, "check failed: missed op")
		}

		var a interface{}
		if err = bson.Unmarshal(cur.Current, &a); err != nil {
			return errors.WithMessage(err, "unmarshal cur.Current")
		}

		var b interface{}
		if err = bson.Unmarshal(doc, &b); err != nil {
			return errors.WithMessage(err, "unmarshal doc")
		}

		if !reflect.DeepEqual(a, b) {
			return errors.New("not the same record")
		}
	}
	if err := cur.Err(); err != nil {
		return errors.WithMessage(err, "cursor")
	}

	if src.LoadNext() != nil {
		return errors.New("check failed: extra record")
	}

	if err := src.Err(); err != nil {
		return errors.WithMessage(err, "check failed: load last bson: unexpected err")
	}

	return nil
}
