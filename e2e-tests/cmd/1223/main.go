package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

const (
	adminURI  = "mongodb://adm:pass@mongos:27017"
	backupURI = "mongodb://bcp:pass@mongos:27017"
)

const dataSize = 10 * 1024

func main() {
	ctx := context.Background()

	m, err := mongo.Connect(ctx, options.Client().ApplyURI(adminURI))
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer func() {
		if err := m.Disconnect(context.Background()); err != nil {
			log.Fatalf("mongo disconnect: %v", err)
		}
		fmt.Println("mongo disconnected")
	}()

	if err := shardCollection(ctx, m); err != nil {
		log.Fatalf("shard collection: %v", err)
	}
	defer func() {
		if err := m.Database("db0").Drop(context.Background()); err != nil {
			log.Fatalf("drop database: %v", err)
		}
		log.Printf("database %q dropped\n", "db0")
	}()

	doneC := make(chan struct{})

	makeBackup := sync.OnceFunc(func() {
		fmt.Println("running backup")
		err := exec.Command("pbm", "--mongodb-uri", backupURI, "backup", "--wait").Run()
		if err != nil {
			log.Printf("run backup: %v\n", err)
		}

		log.Println("run backup: Successful")
		close(doneC)
	})

	wg := sync.WaitGroup{}
	wg.Add(2)

	eg, grpCtx := errgroup.WithContext(ctx)
	var d int64
	for i := 0; i != 100; i++ {
		eg.Go(func() error {
			err := seedData(grpCtx, makeGenDoc(&d), sync.OnceFunc(func() {
				wg.Done()
				wg.Wait()

				go makeBackup()
				time.Sleep(1)
			}))
			return errors.Wrap(err, "populate db")
		})
	}
	if err := eg.Wait(); err != nil {
		log.Fatalf("wait: %v", err)
	}

	<-doneC

	fmt.Println("done")
}

type GenDocFn func() (bson.D, error)

func makeGenDoc(d *int64) GenDocFn {
	buf := make([]byte, dataSize)
	return func() (bson.D, error) {
		n, err := rand.Read(buf)
		if err != nil {
			return nil, err
		}
		if n != cap(buf) {
			return nil, io.ErrShortWrite
		}

		data := []byte(base64.StdEncoding.EncodeToString(buf))
		doc := bson.D{
			{"i", atomic.AddInt64(d, 1)},
			{"data", primitive.Binary{Data: data}},
		}

		return doc, nil
	}
}

func shardCollection(ctx context.Context, m *mongo.Client) error {
	if err := m.Database("db0").Drop(context.Background()); err != nil {
		log.Fatalf("drop database: %v", err)
	}
	log.Printf("database %q dropped\n", "db0")

	res := m.Database("admin").RunCommand(ctx, bson.D{{"enableSharding", "db0"}})
	if err := res.Err(); err != nil {
		return errors.Wrapf(err, "enableSharding %q", "db0")
	}

	res = m.Database("admin").RunCommand(ctx, bson.D{
		{"shardCollection", "db0.coll0"},
		{"key", bson.M{"_id": "hashed"}},
	})
	if err := res.Err(); err != nil {
		return errors.Wrapf(err, "shardCollection %q", "db0.coll0")
	}

	return nil
}

func seedData(ctx context.Context, genDoc GenDocFn, runBackup func()) error {
	m, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://adm:pass@mongos:27017"))
	if err != nil {
		return errors.Wrap(err, "mongo connect")
	}
	defer func() {
		if err := m.Disconnect(context.Background()); err != nil {
			log.Fatalf("mongo disconnect: %v", err)
		}
		fmt.Println("mongo disconnected")
	}()

	err = m.UseSession(ctx, func(ctx mongo.SessionContext) error {
		_, err := ctx.WithTransaction(ctx, func(ctx mongo.SessionContext) (any, error) {
			coll := m.Database("db0").Collection("coll0")
			for i := 0; i != 100; i++ {
				doc, err := genDoc()
				if err != nil {
					return nil, errors.Wrap(err, "gen doc")
				}
				if _, err := coll.InsertOne(ctx, doc); err != nil {
					return nil, errors.Wrapf(err, "insert %d", i)
				}
			}

			runBackup()

			for i := 100; i != 300; i++ {
				doc, err := genDoc()
				if err != nil {
					return nil, errors.Wrap(err, "gen doc")
				}
				if _, err := coll.InsertOne(ctx, doc); err != nil {
					return nil, errors.Wrapf(err, "insert %d", i)
				}
			}

			return nil, ctx.CommitTransaction(ctx)
		})

		return errors.Wrap(err, "trx")
	})
	if err != nil {
		log.Fatalf("mongo session: %v", err)
	}

	return nil
}
