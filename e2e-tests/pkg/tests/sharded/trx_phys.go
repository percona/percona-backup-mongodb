package sharded

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

func (c *Cluster) DistributedTransactionsPhys(bcp Backuper, col string) {
	const trxLimitT = 300

	dbcol := trxdb + "." + col

	ctx := context.Background()
	conn := c.mongos.Conn()

	log.Println("Updating transactionLifetimeLimitSeconds to", trxLimitT)
	err := c.mongopbm.Conn().AdminCommand(
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

	c.setupTrxCollection(ctx, col)

	_, err = conn.Database(trxdb).Collection(col).DeleteMany(ctx, bson.M{})
	if err != nil {
		log.Fatalf("ERROR: delete data from %s: %v", dbcol, err)
	}

	err = c.mongos.GenData(trxdb, col, 0, 5000)
	if err != nil {
		log.Fatalln("ERROR: GenData:", err)
	}

	txnOpts := options.Transaction().
		SetReadPreference(readpref.Primary()).
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(writeconcern.Majority())
	sess, err := conn.StartSession(
		options.Session().
			SetCausalConsistency(true).
			SetDefaultTransactionOptions(txnOpts))
	if err != nil {
		log.Fatalln("ERROR: start session:", err)
	}
	defer sess.EndSession(ctx)

	err = conn.Database("admin").RunCommand(
		ctx,
		bson.D{
			{"moveChunk", dbcol},
			{"find", bson.M{"idx": 2000}},
			{"to", "rsx"},
		},
	).Err()
	if err != nil {
		log.Printf("ERROR: moveChunk %s/idx:2000: %v", dbcol, err)
	}

	c.printBalancerStatus(ctx)

	log.Println("Starting a backup")
	go bcp.Backup()

	// distributed transaction that commits before the backup ends
	// should be visible after restore
	log.Println("Run trx1")
	_, _ = sess.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
		c.trxSet(ctx, 30, col)
		c.trxSet(ctx, 530, col)
		c.trxSet(ctx, 130, col)
		c.trxSet(ctx, 131, col)
		c.trxSet(ctx, 630, col)
		c.trxSet(ctx, 631, col)
		c.trxSet(ctx, 110, col)
		c.trxSet(ctx, 730, col)
		c.trxSet(ctx, 3000, col)
		c.trxSet(ctx, 3001, col)

		return nil, nil //nolint:nilnil
	})

	bcp.WaitStarted()

	log.Println("Run trx2")
	// distributed transaction that commits after the backup ends
	// should NOT be visible after the restore
	_ = mongo.WithSession(ctx, sess, func(ctx context.Context) error {
		err := sess.StartTransaction()
		if err != nil {
			log.Fatalln("ERROR: start transaction:", err)
		}
		defer func() {
			if err != nil {
				_ = sess.AbortTransaction(ctx)
				log.Fatalln("ERROR: transaction:", err)
			}
		}()

		c.trxSet(ctx, 0, col)
		c.trxSet(ctx, 89, col)
		c.trxSet(ctx, 180, col)

		log.Println("Waiting for the backup to done")
		bcp.WaitDone()
		log.Println("Backup done")

		c.trxSet(ctx, 99, col)
		c.trxSet(ctx, 199, col)
		c.trxSet(ctx, 2001, col)

		log.Println("Committing the transaction")
		err = sess.CommitTransaction(ctx)
		if err != nil {
			log.Fatalln("ERROR: commit in transaction:", err)
		}

		return nil
	})
	sess.EndSession(ctx)

	c.checkTrxCollection(ctx, col, bcp)
}
