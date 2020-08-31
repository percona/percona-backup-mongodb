package sharded

import (
	"context"
	"log"
	"strings"
	"time"

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

func (c *Cluster) DistributedTransactions(bcp Backuper) {
	ctx := context.Background()
	conn := c.mongos.Conn()

	const trxLimitT = 300
	log.Println("Updating transactionLifetimeLimitSeconds to", trxLimitT)
	err := c.mongopbm.Conn().Database("admin").RunCommand(
		ctx,
		bson.D{{"setParameter", 1}, {"transactionLifetimeLimitSeconds", trxLimitT}},
	).Err()
	if err != nil {
		log.Fatalln("ERROR: update transactionLifetimeLimitSeconds:", err)
	}
	for sname, cn := range c.shards {
		log.Printf("Updating transactionLifetimeLimitSeconds for %s to %d", sname, trxLimitT)
		err := cn.Conn().Database("admin").RunCommand(
			ctx,
			bson.D{{"setParameter", 1}, {"transactionLifetimeLimitSeconds", trxLimitT}},
		).Err()
		if err != nil {
			log.Fatalf("ERROR: update transactionLifetimeLimitSeconds for shard %s: %v", sname, err)
		}
	}

	c.setupTrxCollection(ctx)

	c.moveChunk(ctx, 0, "rs1")
	c.moveChunk(ctx, 30, "rs1")
	c.moveChunk(ctx, 89, "rs1")
	c.moveChunk(ctx, 99, "rs1")
	c.moveChunk(ctx, 110, "rs1")
	c.moveChunk(ctx, 130, "rs1")
	c.moveChunk(ctx, 131, "rs1")
	c.moveChunk(ctx, 630, "rs2")
	c.moveChunk(ctx, 530, "rs2")
	c.moveChunk(ctx, 631, "rs2")
	c.moveChunk(ctx, 730, "rs2")
	c.moveChunk(ctx, 3000, "rs2")
	c.moveChunk(ctx, 3001, "rs2")
	c.moveChunk(ctx, 180, "rs2")
	c.moveChunk(ctx, 199, "rs2")
	c.moveChunk(ctx, 2001, "rs2")

	_, err = conn.Database("trx").Collection("test").DeleteMany(ctx, bson.M{})
	if err != nil {
		log.Fatalln("ERROR: delete data from trx.test:", err)
	}

	err = c.mongos.GenData("trx", "test", 5000)
	if err != nil {
		log.Fatalln("ERROR: GenData:", err)
	}

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

	err = conn.Database("admin").RunCommand(
		ctx,
		bson.D{
			{"moveChunk", "trx.test"},
			{"find", bson.M{"idx": 2000}},
			{"to", "rs2"},
		},
	).Err()
	if err != nil {
		log.Println("ERROR: moveChunk trx.test/idx:2000:", err)
	}

	c.printBalancerStatus(ctx)

	log.Println("Starting a backup")
	go bcp.Backup()

	// distributed transaction that commits before the backup ends
	// should be visible after restore
	log.Println("Run trx1")
	sess.WithTransaction(ctx, func(sc mongo.SessionContext) (interface{}, error) {
		c.trxSet(sc, 30)
		c.trxSet(sc, 530)

		bcp.WaitStarted()

		c.trxSet(sc, 130)
		c.trxSet(sc, 131)
		c.trxSet(sc, 630)
		c.trxSet(sc, 631)

		bcp.WaitSnapshot()

		c.trxSet(sc, 110)
		c.trxSet(sc, 730)
		c.trxSet(sc, 3000)
		c.trxSet(sc, 3001)

		return nil, nil
	})

	log.Println("Run trx2")
	// distributed transaction that commits after the backup ends
	// should NOT be visible after the restore
	mongo.WithSession(ctx, sess, func(sc mongo.SessionContext) error {
		err := sess.StartTransaction()
		if err != nil {
			log.Fatalln("ERROR: start transaction:", err)
		}
		defer func() {
			if err != nil {
				sess.AbortTransaction(sc)
				log.Fatalln("ERROR: transaction:", err)
			}
		}()

		c.trxSet(sc, 0)
		c.trxSet(sc, 89)
		c.trxSet(sc, 180)

		c.printBalancerStatus(ctx)

		log.Println("Waiting for the backup to done")
		bcp.WaitDone()
		log.Println("Backup done")

		c.printBalancerStatus(ctx)

		c.trxSet(sc, 99)
		c.trxSet(sc, 199)
		c.trxSet(sc, 2001)

		log.Println("Commiting the transaction")
		err = sess.CommitTransaction(sc)
		if err != nil {
			log.Fatalln("ERROR: commit in transaction:", err)
		}

		return nil
	})
	sess.EndSession(ctx)

	c.printBalancerStatus(ctx)

	c.checkTrxCollection(ctx, bcp)
}

func (c *Cluster) trxSet(ctx mongo.SessionContext, id int) {
	log.Print("\ttrx", id)
	err := c.updateTrxRetry(ctx, bson.M{"idx": id}, bson.D{{"$set", bson.M{"changed": 1}}})
	if err != nil {
		log.Fatalf("ERROR: update in transaction trx%d: %v", id, err)
	}
}

// updateTrxRetry tries to run an update operation and in case of the StaleConfig error
// it run flushRouterConfig and tries again
func (c *Cluster) updateTrxRetry(ctx mongo.SessionContext, filter interface{}, update interface{}) error {
	var err error
	conn := c.mongos.Conn()
	for i := 0; i < 3; i++ {
		c.flushRouterConfig(context.Background())
		_, err = conn.Database("trx").Collection("test").UpdateOne(ctx, filter, update)
		if err == nil {
			return nil
		}
		if !strings.Contains(err.Error(), "StaleConfig") {
			return err
		}
	}
	return err
}

func (c *Cluster) printBalancerStatus(ctx context.Context) {
	br := c.mongos.Conn().Database("admin").RunCommand(
		ctx,
		bson.D{{"balancerStatus", 1}},
	)

	state, err := br.DecodeBytes()
	if err != nil {
		log.Fatalln("ERROR: balancerStatus:", err)
	}
	log.Println("Ballancer status:", state)
}

func (c *Cluster) flushRouterConfig(ctx context.Context) {
	log.Println("flushRouterConfig")
	err := c.mongos.Conn().Database("admin").RunCommand(
		ctx,
		bson.D{{"flushRouterConfig", 1}},
	).Err()
	if err != nil {
		log.Fatalln("ERROR: flushRouterConfig:", err)
	}
}

func (c *Cluster) deleteTrxData(ctx context.Context, tout time.Duration) bool {
	log.Println("Deleting trx.test data")
	timer := time.NewTimer(tout)
	defer timer.Stop()
	tk := time.NewTicker(time.Second * 1)
	defer tk.Stop()
	for {
		select {
		case <-timer.C:
			log.Printf("Warning: unable to drop trx.test. %v timeout exceeded", tout)
			return false
		case <-tk.C:
			err := c.mongos.Conn().Database("trx").Collection("test").Drop(ctx)
			if err != nil && !strings.Contains(err.Error(), "LockBusy") {
				log.Fatalln("ERROR: drop trx.test collections:", err)
			}
			return true
		}
	}
}

func (c *Cluster) setupTrxCollection(ctx context.Context) {
	conn := c.mongos.Conn()

	err := conn.Database("trx").Collection("test").Drop(ctx)
	if err != nil {
		log.Fatalln("ERROR: drop old trx.test collections:", err)
	}

	log.Println("Creating a sharded collection")
	err = conn.Database("trx").RunCommand(
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
		bson.D{{"updateZoneKeyRange", "trx.test"}, {"min", bson.M{"idx": 0}}, {"max", bson.M{"idx": 151}}, {"zone", "R1"}},
	).Err()
	if err != nil {
		log.Fatalln("ERROR: updateZoneKeyRange trx.test/R1:", err)
	}
	err = conn.Database("admin").RunCommand(
		ctx,
		bson.D{{"updateZoneKeyRange", "trx.test"}, {"min", bson.M{"idx": 151}}, {"max", bson.M{"idx": 1000}}, {"zone", "R2"}},
	).Err()
	if err != nil {
		log.Fatalln("ERROR: updateZoneKeyRange trx.test/R2:", err)
	}
}

func (c *Cluster) moveChunk(ctx context.Context, idx int, to string) {
	log.Println("move chunk", idx, "to", to)
	err := c.mongos.Conn().Database("admin").RunCommand(
		ctx,
		bson.D{
			{"moveChunk", "trx.test"},
			{"find", bson.M{"idx": idx}},
			{"to", to},
		},
	).Err()
	if err != nil {
		log.Println("ERROR: moveChunk trx.test/idx:2000:", err)
	}
}

func (c *Cluster) checkTrxCollection(ctx context.Context, bcp Backuper) {
	log.Println("Checking restored data")

	c.DeleteBallast()
	if ok := c.deleteTrxData(ctx, time.Minute*1); !ok {
		c.zeroTrxDoc(ctx, 30)
		c.zeroTrxDoc(ctx, 530)
		c.zeroTrxDoc(ctx, 130)
		c.zeroTrxDoc(ctx, 131)
		c.zeroTrxDoc(ctx, 630)
		c.zeroTrxDoc(ctx, 631)
		c.zeroTrxDoc(ctx, 110)
		c.zeroTrxDoc(ctx, 730)
		c.zeroTrxDoc(ctx, 3000)
		c.zeroTrxDoc(ctx, 3001)
		c.zeroTrxDoc(ctx, 0)
		c.zeroTrxDoc(ctx, 89)
		c.zeroTrxDoc(ctx, 99)
		c.zeroTrxDoc(ctx, 180)
		c.zeroTrxDoc(ctx, 199)
		c.zeroTrxDoc(ctx, 2001)
	}
	c.flushRouterConfig(ctx)

	bcp.Restore()

	log.Println("check commited transaction")
	c.checkTrxDoc(ctx, 30, 1)
	c.checkTrxDoc(ctx, 530, 1)
	c.checkTrxDoc(ctx, 130, 1)
	c.checkTrxDoc(ctx, 131, 1)
	c.checkTrxDoc(ctx, 630, 1)
	c.checkTrxDoc(ctx, 631, 1)
	c.checkTrxDoc(ctx, 110, 1)
	c.checkTrxDoc(ctx, 730, 1)
	c.checkTrxDoc(ctx, 3000, 1)
	c.checkTrxDoc(ctx, 3001, 1)

	log.Println("check uncommited (commit wasn't dropped to backup) transaction")
	c.checkTrxDoc(ctx, 0, -1)
	c.checkTrxDoc(ctx, 89, -1)
	c.checkTrxDoc(ctx, 99, -1)
	c.checkTrxDoc(ctx, 180, -1)
	c.checkTrxDoc(ctx, 199, -1)
	c.checkTrxDoc(ctx, 2001, -1)

	log.Println("check data that wasn't touched by transactions")
	c.checkTrxDoc(ctx, 10, -1)
	c.checkTrxDoc(ctx, 2000, -1)
}

func (c *Cluster) zeroTrxDoc(ctx context.Context, id int) {
	_, err := c.mongos.Conn().Database("trx").Collection("test").UpdateOne(ctx, bson.M{"idx": id}, bson.D{{"$set", bson.M{"changed": 0}}})
	if err != nil {
		log.Fatalf("ERROR: update idx %v: %v", id, err)
	}
}

func (c *Cluster) checkTrxDoc(ctx context.Context, id, expect int) {
	log.Println("\tcheck", id, expect)
	r1 := pbm.TestData{}
	err := c.mongos.Conn().Database("trx").Collection("test").FindOne(ctx, bson.M{"idx": id}).Decode(&r1)
	if err != nil {
		log.Fatalf("ERROR: get trx.test record `idx %v`: %v", id, err)
	}
	if r1.C != expect {
		log.Fatalf("ERROR: wrong trx.test record `idx %v`. got: %v, expect: %v\n", id, r1.C, expect)
	}
}
