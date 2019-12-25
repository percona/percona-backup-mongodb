package pbm

import (
	"context"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"gopkg.in/mgo.v2/bson"
)

type Mongo struct {
	cn  *mongo.Client
	ctx context.Context
}

func NewMongo(ctx context.Context, connectionURI string) (*Mongo, error) {
	cn, err := connect(ctx, connectionURI, "e2e-tests")
	if err != nil {
		return nil, errors.Wrap(err, "connect")
	}

	return &Mongo{
		cn:  cn,
		ctx: ctx,
	}, nil
}

func connect(ctx context.Context, uri, appName string) (*mongo.Client, error) {
	client, err := mongo.NewClient(
		options.Client().ApplyURI(uri).
			SetAppName(appName).
			SetReadPreference(readpref.Primary()).
			SetReadConcern(readconcern.Majority()).
			SetWriteConcern(writeconcern.New(writeconcern.WMajority())),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create mongo client")
	}
	err = client.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "mongo connect")
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, errors.Wrap(err, "mongo ping")
	}

	return client, nil
}

type Generator struct {
	Err    chan error
	Count  chan int
	LastTS chan time.Time
	LastID chan interface{}
}

const (
	testDB         = "test"
	testCollection = "test"
)

// GenerateData inserts data each duration until the ctx cancelation recieved
// or count achieved. count < 0 means no count takes into account
func (m *Mongo) GenerateData(ctx context.Context, each time.Duration, count int) *Generator {
	tk := time.NewTicker(each)
	g := &Generator{}
	go func() {
		ins := struct {
			IDX  int       `bson:"idx"`
			Time time.Time `bson:"time"`
			TS   int64     `bson:"ts"`
		}{}
		for i := 0; i != count; i++ {
			select {
			case <-tk.C:
				ins.IDX = i
				ins.Time = time.Now()
				ins.TS = ins.Time.Unix()
				r, err := m.cn.Database(testDB).Collection(testCollection).InsertOne(m.ctx, ins)
				if err != nil {
					g.Err <- err
					return
				}
				g.LastID <- r.InsertedID
				g.LastTS <- ins.Time
				g.Count <- i
			case <-ctx.Done():
				tk.Stop()
				return
			}
		}
	}()

	return g
}

func (m *Mongo) Fill(ln int) error {
	var data []interface{}
	for i := 0; i < ln/100; i++ {
		data = append(data, *genBallast(100))
		if i%100 == 0 {
			_, err := m.cn.Database(testDB).Collection("ballast").InsertMany(m.ctx, data)
			if err != nil {
				return err
			}
			data = data[:0]
		}
	}
	if len(data) > 0 {
		_, err := m.cn.Database(testDB).Collection("ballast").InsertMany(m.ctx, data)
		return err
	}

	return nil
}

func genBallast(ln int) *bson.M {
	l1 := make([]byte, ln)
	l2 := make([]byte, ln)
	d := make([]int64, ln)

	for i := 0; i < ln; i++ {
		d[i] = rand.Int63()
		l1[i] = byte(d[i]&25 + 'a')
		l2[i] = byte(d[i]&25 + 'A')
	}

	return &bson.M{
		"letters1": l1,
		"letters2": l2,
		// "digits":   d,
	}
}

func (m *Mongo) DeleteData() (int, error) {
	r, err := m.cn.Database(testDB).Collection(testCollection).DeleteMany(m.ctx, bson.M{})
	if err != nil {
		return 0, err
	}
	return int(r.DeletedCount), nil
}

func (m *Mongo) Hashes() (map[string]string, error) {
	r := m.cn.Database(testDB).RunCommand(m.ctx, bson.M{"dbHash": 1})
	if r.Err() != nil {
		return nil, errors.Wrap(r.Err(), "run command")
	}
	h := struct {
		C map[string]string `bson:"collections"`
		M string            `bson:"md5"`
	}{}
	err := r.Decode(&h)
	h.C["_all_"] = h.M

	return h.C, errors.Wrap(err, "decode")
}
