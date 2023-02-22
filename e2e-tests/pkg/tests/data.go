package tests

import (
	"context"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/errgroup"
)

type GenDBSpec struct {
	Name  string
	Colls []GenCollSpec
}

type GenCollSpec struct {
	Name        string
	ShardingKey *ShardingOptions
}

type ShardingOptions struct {
	Key map[string]any
}

func Deploy(ctx context.Context, m *mongo.Client, dbs []GenDBSpec) error {
	ok, err := isMongos(ctx, m)
	if err != nil {
		return errors.WithMessage(err, "ismongos")
	}
	if !ok {
		return errors.New("mongos connection required")
	}

	adm := m.Database("admin")

	for _, db := range dbs {
		sharded := false

		for _, coll := range db.Colls {
			if coll.ShardingKey == nil || len(coll.ShardingKey.Key) == 0 {
				continue
			}

			if !sharded {
				res := adm.RunCommand(ctx, bson.D{
					{"enableSharding", db.Name},
				})
				if err := res.Err(); err != nil {
					return err
				}
				sharded = true
			}

			res := adm.RunCommand(ctx, bson.D{
				{"shardCollection", db.Name + "." + coll.Name},
				{"key", coll.ShardingKey.Key},
				{"numInitialChunks", 11},
			})
			if err := res.Err(); err != nil {
				return err
			}
		}
	}

	return nil
}

func GenerateData(ctx context.Context, m *mongo.Client, dbs []GenDBSpec) error {
	ok, err := isMongos(ctx, m)
	if err != nil {
		return errors.WithMessage(err, "ismongos")
	}
	if !ok {
		return errors.New("mongos connection required")
	}

	eg, egc := errgroup.WithContext(ctx)
	eg.SetLimit(8)

	for _, d := range dbs {
		db := m.Database(d.Name)
		for _, c := range d.Colls {
			coll := db.Collection(c.Name)
			docs := make([]any, rand.Int()%400+100)

			eg.Go(func() error {
				r := rand.New(rand.NewSource(time.Now().Unix()))
				for i := range docs {
					docs[i] = bson.M{"i": i, "r": r.Int(), "t": time.Now()}
				}

				_, err := coll.InsertMany(egc, docs)
				return err
			})
		}
	}

	return eg.Wait()
}
