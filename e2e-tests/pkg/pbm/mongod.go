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

const (
	testDB = "test"
	// testCollection = "test"
)

// // GenerateData inserts data each duration until the ctx cancelation recieved
// // or count achieved. count < 0 means no count takes into account
// func (m *Mongo) GenerateData(ctx context.Context, each time.Duration, count int) *Generator {
// 	tk := time.NewTicker(each)
// 	g := &Generator{}
// 	go func() {
// 		ins := struct {
// 			IDX  int       `bson:"idx"`
// 			Time time.Time `bson:"time"`
// 			TS   int64     `bson:"ts"`
// 		}{}
// 		for i := 0; i != count; i++ {
// 			select {
// 			case <-tk.C:
// 				ins.IDX = i
// 				ins.Time = time.Now()
// 				ins.TS = ins.Time.Unix()
// 				r, err := m.cn.Database(testDB).Collection(testCollection).InsertOne(m.ctx, ins)
// 				if err != nil {
// 					g.Err <- err
// 					return
// 				}
// 				g.Data <- &GeneratorData{
// 					LastID: r.InsertedID,
// 					LastTS: ins.Time,
// 					Count:  i,
// 				}
// 			case <-ctx.Done():
// 				tk.Stop()
// 				return
// 			}
// 		}
// 	}()

// 	return g
// }

type InBcpStatus int

const (
	InBcpUndef InBcpStatus = 0
	InBcpYES               = 1
	InBcpNO                = 2
)

// type Generator struct {
// 	Err  chan error
// 	Data chan []GeneratorData
// }

// type GeneratorData struct {
// 	Count  int
// 	TS     time.Time
// 	ID     interface{}
// 	Status InBcpStatus
// }

// // GenerateData inserts data each duration until the ctx cancelation recieved
// // or count achieved. count < 0 means no count takes into account
// func (m *Mongo) GenerateData(ctx context.Context, each time.Duration) *Generator {
// 	tk := time.NewTicker(each)
// 	g := &Generator{
// 		Err:  make(chan error),
// 		Data: make(chan []GeneratorData, 1),
// 	}
// 	var data []GeneratorData
// 	go func() {
// 		ins := struct {
// 			IDX  int       `bson:"idx"`
// 			Time time.Time `bson:"time"`
// 			TS   int64     `bson:"ts"`
// 		}{}
// 		for i := 0; ; i++ {
// 			select {
// 			case <-tk.C:
// 				ins.IDX = i
// 				ins.Time = time.Now()
// 				ins.TS = ins.Time.Unix()

// 				r, err := m.cn.Database(testDB).Collection(testCollection).InsertOne(m.ctx, ins)
// 				if err != nil {
// 					g.Err <- err
// 					return
// 				}
// 				data = append(data, GeneratorData{
// 					ID:    r.InsertedID,
// 					TS:    ins.Time,
// 					Count: i,
// 				})
// 			case <-ctx.Done():
// 				tk.Stop()
// 				g.Data <- data
// 				return
// 			}
// 		}
// 	}()

// 	return g
// }

func (m *Mongo) ResetData() (int, error) {
	r, err := m.cn.Database(testDB).Collection("counter").DeleteMany(m.ctx, bson.M{})
	if err != nil {
		return 0, err
	}
	return int(r.DeletedCount), nil
}

func (m *Mongo) Fill(ln int) error {
	var data []interface{}
	for i := 0; i < ln; i++ {
		data = append(data, *genBallast(50))
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

func (m *Mongo) ResetBallast() (int, error) {
	r, err := m.cn.Database(testDB).Collection("ballast").DeleteMany(m.ctx, bson.M{})
	if err != nil {
		return 0, err
	}
	return int(r.DeletedCount), nil
}

func genBallast(strLen int) *bson.M {
	l1 := make([]byte, strLen)
	l2 := make([]byte, strLen)
	d := make([]int64, strLen)

	for i := 0; i < strLen; i++ {
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

type TestData struct {
	Count  int
	TS     time.Time
	ID     interface{}
	Status InBcpStatus
}

func (m *Mongo) WriteData(i int) (*TestData, error) {
	ins := struct {
		IDX  int       `bson:"idx"`
		Time time.Time `bson:"time"`
		TS   int64     `bson:"ts"`
	}{
		IDX:  i,
		Time: time.Now(),
	}

	ins.TS = ins.Time.Unix()
	r, err := m.cn.Database(testDB).Collection("counter").InsertOne(m.ctx, ins)
	if err != nil {
		return nil, err
	}

	return &TestData{
		Count: i,
		ID:    r.InsertedID,
		TS:    ins.Time,
	}, nil
}

func (m *Mongo) GetData() ([]TestData, error) {
	cur, err := m.cn.Database(testDB).Collection("counter").Find(
		m.ctx,
		bson.M{},
		options.Find().SetSort(bson.M{"idx": 1}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create cursor failed")
	}
	defer cur.Close(m.ctx)

	var data []TestData
	for cur.Next(m.ctx) {
		t := struct {
			ID   interface{} `bson:"_id"`
			IDX  int         `bson:"idx"`
			Time time.Time   `bson:"time"`
			TS   int64       `bson:"ts"`
		}{}
		err := cur.Decode(&t)
		if err != nil {
			return nil, errors.Wrap(err, "decode data")
		}
		data = append(data, TestData{
			Count: t.IDX,
			ID:    t.ID,
			TS:    time.Unix(t.TS, 0), //ins.Time,
		})
	}

	return data, nil
}
