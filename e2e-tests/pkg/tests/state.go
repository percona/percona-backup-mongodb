package tests

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"log"
	"reflect"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm/sel"
)

type clusterState struct {
	Config map[string]configDBState `json:"config"`
	Shards map[string]shardState    `json:"shards"`
	Counts map[string]int64         `json:"counts"`
}

type configDBState struct {
	Spec  *dbSpec                     `json:"spec"`
	Colls map[string]*configCollState `json:"collections"`
}

type dbSpec struct {
	ID      string    `bson:"_id"`
	Primary string    `bson:"primary"`
	Version dbVersion `bson:"version"`
}

type dbVersion struct {
	UUID      primitive.Binary    `bson:"uuid"`
	Timestamp primitive.Timestamp `bson:"timestamp"`
	LastMod   int32               `bson:"lastMod"`
}

type configCollState struct {
	Spec *collSpec `json:"spec"`
	Hash string    `json:"hash"`
}

type collSpec struct {
	ID           string              `bson:"_id"`
	LastmodEpoch primitive.ObjectID  `bson:"lastmodEpoch"`
	LastMod      primitive.DateTime  `bson:"lastMod"`
	Timestamp    primitive.Timestamp `bson:"timestamp"`
	UUID         primitive.Binary    `bson:"uuid"`
	Key          map[string]any      `bson:"key"`
	Unique       bool                `bson:"unique"`
	ChunksSplit  bool                `bson:"chunksAlreadySplitForDowngrade"`
	NoBalance    bool                `bson:"noBalance"`
}

type chunkHistoryItem struct {
	ValidAfter primitive.Timestamp `bson:"validAfter"`
	Shard      string              `bson:"shard"`
}

type shardState map[string]*shardCollState

type shardCollState struct {
	Spec *mongo.CollectionSpecification `json:"spec"`
	Hash string                         `json:"hash"`
}

type Credentials struct {
	Username, Password string
}

func ExtractCredentionals(s string) *Credentials {
	if strings.HasPrefix(s, "mongodb://") {
		s = s[len("mongodb://"):]
	}

	auth, _, _ := strings.Cut(s, "@")
	usr, pwd, _ := strings.Cut(auth, ":")
	if usr == "" {
		return nil
	}

	return &Credentials{usr, pwd}
}

func ClusterState(ctx context.Context, mongos *mongo.Client, creds *Credentials) (*clusterState, error) {
	ok, err := isMongos(ctx, mongos)
	if err != nil {
		return nil, errors.WithMessage(err, "ismongos")
	}
	if !ok {
		return nil, errors.New("mongos connection required")
	}

	res := mongos.Database("admin").RunCommand(ctx, bson.D{{"getShardMap", 1}})
	if err := res.Err(); err != nil {
		return nil, errors.WithMessage(err, "getShardMap: query")
	}

	var shardMap struct{ Map map[string]string }
	if err := res.Decode(&shardMap); err != nil {
		return nil, errors.WithMessage(err, "getShardMap: decode")
	}

	rv := &clusterState{
		Shards: make(map[string]shardState),
		Counts: make(map[string]int64),
	}

	eg, egc := errgroup.WithContext(ctx)
	eg.Go(func() (err error) {
		rv.Counts, err = countDocuments(egc, mongos)
		return errors.WithMessage(err, "count documents")
	})

	mu := sync.Mutex{}
	for rs, uri := range shardMap.Map {
		rs := rs
		_, uri, _ := strings.Cut(uri, "/")
		if creds != nil {
			uri = fmt.Sprintf("%s:%s@%s", creds.Username, creds.Password, uri)
		}
		uri = "mongodb://" + uri

		eg.Go(func() error {
			m, err := mongo.Connect(egc, options.Client().ApplyURI(uri))
			if err != nil {
				return errors.WithMessagef(err, "connect: %q", uri)
			}

			if rs == "config" {
				rv.Config, err = getConfigState(egc, m)
				return errors.WithMessagef(err, "config state: %q", uri)
			}

			state, err := getShardState(egc, m)
			if err != nil {
				return errors.WithMessagef(err, "shard state: %q", uri)
			}

			mu.Lock()
			defer mu.Unlock()

			rv.Shards[rs] = state
			return nil
		})
	}

	err = eg.Wait()
	return rv, err
}

func Compare(before, after *clusterState, nss []string) bool {
	ok := true
	allowedDBs := make(map[string]bool)
	for _, ns := range nss {
		db, _, _ := strings.Cut(ns, ".")
		if db == "*" {
			db = ""
		}
		allowedDBs[db] = true
	}
	selected := sel.MakeSelectedPred(nss)

	for db, beforeDBState := range before.Config {
		if !allowedDBs[""] && !allowedDBs[db] {
			continue
		}

		afterDBState := after.Config[db]
		if !reflect.DeepEqual(beforeDBState, afterDBState) {
			log.Printf("config: diff database spec: %q", db)
			ok = false
		}

		for coll, beforeCollState := range beforeDBState.Colls {
			if !selected(db + "." + coll) {
				continue
			}

			afterCollState := afterDBState.Colls[coll]
			if !reflect.DeepEqual(beforeCollState, afterCollState) {
				log.Printf("config: diff namespace spec: %q", coll)
				ok = false
			}
		}
	}

	for shard, beforeShardState := range before.Shards {
		afterShardState, ok := after.Shards[shard]
		if !ok {
			log.Printf("shard: not found: %q", shard)
			ok = false
			continue
		}

		for ns, beforeNSState := range beforeShardState {
			if !selected(ns) {
				continue
			}

			if !reflect.DeepEqual(beforeNSState, afterShardState[ns]) {
				log.Printf("shard: diff namespace spec: %q", ns)
				ok = false
			}
		}
	}

	for ns, beforeCount := range before.Counts {
		if !selected(ns) {
			continue
		}

		if beforeCount != after.Counts[ns] {
			log.Printf("mongos: diff documents count: %q", ns)
			ok = false
		}
	}

	return ok
}

func isMongos(ctx context.Context, m *mongo.Client) (bool, error) {
	res := m.Database("admin").RunCommand(ctx, bson.D{{"hello", 1}})
	if err := res.Err(); err != nil {
		return false, errors.WithMessage(err, "query")
	}

	var r struct{ Msg string }
	err := res.Decode(&r)
	return r.Msg == "isdbgrid", errors.WithMessage(err, "decode")
}

func countDocuments(ctx context.Context, mongos *mongo.Client) (map[string]int64, error) {
	f := bson.D{{"name", bson.M{"$nin": bson.A{"admin", "config", "local"}}}}
	dbs, err := mongos.ListDatabaseNames(ctx, f)
	if err != nil {
		return nil, errors.WithMessage(err, "list databases")
	}

	rv := make(map[string]int64)

	for _, d := range dbs {
		db := mongos.Database(d)

		colls, err := db.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return nil, errors.WithMessagef(err, "list collections: %q", d)
		}

		for _, c := range colls {
			count, err := db.Collection(c).CountDocuments(ctx, bson.D{})
			if err != nil {
				return nil, errors.WithMessagef(err, "count: %q", d+"."+c)
			}

			rv[d+"."+c] = count
		}
	}

	return rv, nil
}

func getConfigDatabases(ctx context.Context, m *mongo.Client) ([]*dbSpec, error) {
	cur, err := m.Database("config").Collection("databases").Find(ctx, bson.D{})
	if err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	rv := []*dbSpec{}
	err = cur.All(ctx, &rv)
	return rv, errors.WithMessage(err, "cursor: all")
}

func getConfigCollections(ctx context.Context, m *mongo.Client) ([]*collSpec, error) {
	f := bson.D{{"_id", bson.M{"$regex": `^(?!(config|system)\.)`}}}
	cur, err := m.Database("config").Collection("collections").Find(ctx, f)
	if err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	rv := []*collSpec{}
	err = cur.All(ctx, &rv)
	return rv, errors.WithMessage(err, "cursor: all")
}

func getConfigChunkHashes(ctx context.Context, m *mongo.Client, uuids map[string]string) (map[string]string, error) {
	hashes := make(map[string]hash.Hash)
	in := make([]primitive.Binary, 0, len(uuids))
	for uuid := range uuids {
		hashes[uuid] = md5.New()
		in = append(in, primitive.Binary{Subtype: 0x4, Data: []byte(uuid)})
	}

	f := bson.D{{"uuid", bson.M{"$in": in}}}
	cur, err := m.Database("config").Collection("chunks").Find(ctx, f)
	if err != nil {
		return nil, errors.WithMessage(err, "query")
	}
	defer cur.Close(ctx)

	for cur.Next(ctx) {
		_, data := cur.Current.Lookup("uuid").Binary()
		hashes[hex.EncodeToString(data)].(hash.Hash).Write(cur.Current)
	}
	if err := cur.Err(); err != nil {
		return nil, errors.WithMessage(err, "cursor")
	}

	rv := make(map[string]string, len(hashes))
	for k, v := range hashes {
		rv[uuids[k]] = hex.EncodeToString(v.Sum(nil))
	}

	return rv, nil
}

func getConfigState(ctx context.Context, m *mongo.Client) (map[string]configDBState, error) {
	dbs, err := getConfigDatabases(ctx, m)
	if err != nil {
		return nil, errors.WithMessage(err, "databases")
	}

	colls, err := getConfigCollections(ctx, m)
	if err != nil {
		return nil, errors.WithMessage(err, "collections")
	}

	u2c := make(map[string]string, len(colls))
	for _, coll := range colls {
		u2c[hex.EncodeToString(coll.UUID.Data)] = coll.ID
	}

	chunks, err := getConfigChunkHashes(ctx, m, u2c)
	if err != nil {
		return nil, errors.WithMessage(err, "config chunk hashes")
	}

	rv := make(map[string]configDBState, len(dbs))
	for _, spec := range dbs {
		rv[spec.ID] = configDBState{spec, make(map[string]*configCollState)}
	}
	for _, spec := range colls {
		d, c, _ := strings.Cut(spec.ID, ".")
		rv[d].Colls[c] = &configCollState{spec, chunks[spec.ID]}
	}

	return rv, nil
}

func getShardState(ctx context.Context, m *mongo.Client) (shardState, error) {
	f := bson.D{{"name", bson.M{"$nin": bson.A{"admin", "config", "local"}}}}
	res, err := m.ListDatabases(ctx, f)
	if err != nil {
		return nil, errors.WithMessage(err, "list databases")
	}

	rv := make(map[string]*shardCollState, len(res.Databases))
	for i := range res.Databases {
		name := res.Databases[i].Name
		db := m.Database(name)

		colls, err := db.ListCollectionSpecifications(ctx, bson.D{})
		if err != nil {
			return nil, errors.WithMessagef(err, "list collections: %q", name)
		}

		res := db.RunCommand(ctx, bson.D{{"dbHash", 1}})
		if err := res.Err(); err != nil {
			return nil, errors.WithMessagef(err, "dbHash: %q: query", name)
		}

		dbHash := struct{ Collections map[string]string }{}
		if err := res.Decode(&dbHash); err != nil {
			return nil, errors.WithMessagef(err, "dbHash: %q: decode", name)
		}

		for _, coll := range colls {
			rv[name+"."+coll.Name] = &shardCollState{coll, dbHash.Collections[coll.Name]}
		}
	}

	return rv, nil
}
