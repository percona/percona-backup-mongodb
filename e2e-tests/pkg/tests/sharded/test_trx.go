package sharded

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/pbm"
)

type shard struct {
	name string
	cn   *pbm.Mongo
}

type trxData struct {
	Country string
	UID     int
}

func (c *Cluster) DistributedTransactions() {
	// log.Println("peek a random replsets")
	// var rs [2]shard
	// i := 0
	// for name, cn := range c.shards {
	// 	rs[i] = shard{name: name, cn: cn}
	// 	if i == 1 {
	// 		break
	// 	}
	// 	i++
	// }
	// if rs[0].name == "" || rs[1].name == "" {
	// 	log.Fatalln("no shards in cluster")
	// }

	ctx := context.Background()
	// _, err := rs[0].cn.Conn().Database("test").Collection("trx0").InsertOne(ctx, bson.M{"x": 0})
	// if err != nil {
	// 	log.Fatalln("ERROR: prepare collections for trx0:", err)
	// }
	// _, err = rs[1].cn.Conn().Database("test").Collection("trx1").InsertOne(ctx, bson.M{"y": 0})
	// if err != nil {
	// 	log.Fatalln("ERROR: prepare collections for trx1:", err)
	// }

	conn := c.mongos.Conn()

	err := conn.Database("trx").RunCommand(
		ctx,
		bson.D{{"create", "test"}},
	).Err()
	if err != nil {
		log.Fatalln("ERROR: create trx.test collections:", err)
	}

	err = conn.Database("admin").RunCommand(
		ctx,
		bson.D{{"enableSharding", "trx"}},
	).Err()
	if err != nil {
		log.Fatalln("ERROR: enableSharding on trx db:", err)
	}

	err = conn.Database("admin").RunCommand(
		ctx,
		bson.D{{"shardCollection", "trx.test"}, {"key", bson.M{"idx": 1}}},
	).Err()
	if err != nil {
		log.Fatalln("ERROR: shardCollection trx.test:", err)
	}

	err = conn.Database("admin").RunCommand(
		ctx,
		bson.D{{"addShardToZone", "rs1"}, {"zone", "R1"}},
	).Err()
	if err != nil {
		log.Fatalln("ERROR: addShardToZone rs1:", err)
	}

	err = conn.Database("admin").RunCommand(
		ctx,
		bson.D{{"addShardToZone", "rs2"}, {"zone", "R2"}},
	).Err()
	if err != nil {
		log.Fatalln("ERROR: addShardToZone rs2:", err)
	}

	err = conn.Database("admin").RunCommand(
		ctx,
		bson.D{{"updateZoneKeyRange", "trx.test"}, {"min", bson.M{"idx": 0}}, {"max", bson.M{"idx": 51}}, {"zone", "R1"}},
	).Err()
	if err != nil {
		log.Fatalln("ERROR: updateZoneKeyRange trx.test/R1:", err)
	}
	err = conn.Database("admin").RunCommand(
		ctx,
		bson.D{{"updateZoneKeyRange", "trx.test"}, {"min", bson.M{"idx": 51}}, {"max", bson.M{"idx": 100}}, {"zone", "R2"}},
	).Err()
	if err != nil {
		log.Fatalln("ERROR: updateZoneKeyRange trx.test/R2:", err)
	}

	err = c.mongos.GenData("trx", "test", 100)

	sess, err := conn.StartSession(
		options.Session().
			SetDefaultReadPreference(readpref.Primary()).
			SetCausalConsistency(true).
			SetDefaultReadConcern(readconcern.Majority()).
			SetDefaultWriteConcern(writeconcern.New(writeconcern.WMajority())),
	)
	if err != nil {
		log.Fatalln("ERROR: start session:", err)
	}
	defer sess.EndSession(ctx)

	err = mongo.WithSession(ctx, sess, func(sc mongo.SessionContext) error {
		var err error
		defer func() {
			if err != nil {
				sess.AbortTransaction(sc)
				log.Fatalln("ERROR: transaction:", err)
			}
		}()

		_, err = conn.Database("test").Collection("trx0").UpdateOne(sc, bson.D{}, bson.D{{"$set", bson.M{"x": 1}}})
		if err != nil {
			log.Fatalln("ERROR: update in transaction trx0:", err)
		}

		// !!! wait for bcp

		_, err = conn.Database("trx").Collection("test").UpdateOne(sc, bson.M{"idx": 99}, bson.D{{"$set", bson.M{"changed": 1}}})
		if err != nil {
			log.Fatalln("ERROR: update in transaction trx:", err)
		}

		return sess.CommitTransaction(sc)
	})

	// conn.Database("test").Collection("trx0").Drop(ctx)
	// conn.Database("test").Collection("trx1").Drop(ctx)
}
