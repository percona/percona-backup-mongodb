package sharded

import (
	"context"
	"log"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	pbmt "github.com/percona/percona-backup-mongodb/e2e-tests/pkg/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

func (c *Cluster) Timeseries() {
	ts1, err := c.newTS("ts1")
	if err != nil {
		log.Fatalln("create timeseries:", err)
	}

	ts1.gen()

	c.pitrOn()
	defer c.pitrOff()

	bcpName := c.LogicalBackup()

	c.BackupWaitDone(context.TODO(), bcpName)

	time.Sleep(time.Second)

	ts2, err := c.newTS("ts2")
	if err != nil {
		log.Fatalln("create timeseries:", err)
	}

	ts2.gen()

	ds := time.Second * 135
	log.Printf("Generating data for %v", ds)
	time.Sleep(ds)

	ts1.stop()
	ts2.stop()
	time.Sleep(time.Second * 60)
	c.pitrOff()

	time.Sleep(time.Second * 6)

	err = c.mongos.Drop("ts1")
	if err != nil {
		log.Fatalf("ERROR: drop ts1: %v", err)
	}

	err = c.mongos.Drop("ts2")
	if err != nil {
		log.Fatalf("ERROR: drop ts2: %v", err)
	}

	list, err := c.pbm.List()
	if err != nil {
		log.Fatalf("ERROR: get backups/pitr list: %v", err)
	}

	if len(list.PITR.Ranges) == 0 {
		log.Fatalf("ERROR: empty pitr list, expected a range after the last backup")
	}

	applied := c.relaxBucketValidator()
	defer c.restoreBucketValidator(applied)

	c.PITRestore(time.Unix(int64(list.PITR.Ranges[len(list.PITR.Ranges)-1].Range.End), 0))

	ts1c, err := c.mongos.Count("ts1")
	if err != nil {
		log.Fatalf("ERROR: count docs in ts1: %v", err)
	}

	if ts1.count() != uint64(ts1c) {
		log.Fatalf("ERROR: wrong timeseries count, expect %d got %d", ts1.count(), ts1c)
	}

	ts2c, err := c.mongos.Count("ts2")
	if err != nil {
		log.Fatalf("ERROR: count docs in ts2: %v", err)
	}

	if ts2.count() != uint64(ts2c) {
		log.Fatalf("ERROR: wrong timeseries count, expect %d got %d", ts2.count(), ts2c)
	}
}

// bucketValidatorParams are the setParameter names that relax the timeseries
// bucket validator. The name differs between server versions (7.0 has
// timeseriesDisableStrictBucketValidator), so a node gets the first one it
// recognizes.
var bucketValidatorParams = []string{
	"timeseriesLessStrictBucketValidator",
	"timeseriesDisableStrictBucketValidator",
}

type appliedBucketValidator struct {
	node  string
	param string
	run   func(bson.D) error
}

// relaxBucketValidator relaxes the timeseries bucket validator so the restore
// can insert buckets that the strict validator rejects.
func (c *Cluster) relaxBucketValidator() []appliedBucketValidator {
	ctx := context.Background()
	var applied []appliedBucketValidator

	set := func(node string, run func(bson.D) error) {
		for _, param := range bucketValidatorParams {
			err := run(bson.D{{"setParameter", 1}, {param, true}})
			if err == nil {
				log.Printf("%s is set on %s", param, node)
				applied = append(applied, appliedBucketValidator{node: node, param: param, run: run})
				return
			}
			if !isUnrecognizedParam(err) {
				log.Fatalf("ERROR: set %s on %s: %v", param, node, err)
			}
		}
		log.Printf("WARNING: %s recognizes none of the bucket validator parameters", node)
	}

	set("the config server", func(cmd bson.D) error {
		return c.mongopbm.Conn().AdminCommand(ctx, cmd).Err()
	})

	for sname, cn := range c.shards {
		set(sname, func(cmd bson.D) error {
			return cn.Conn().Database("admin").RunCommand(ctx, cmd).Err()
		})
	}
	return applied
}

func (c *Cluster) restoreBucketValidator(applied []appliedBucketValidator) {
	for _, a := range applied {
		err := a.run(bson.D{{"setParameter", 1}, {a.param, false}})
		if err != nil {
			log.Fatalf("ERROR: restore %s on %s: %v", a.param, a.node, err)
		}
		log.Printf("%s is restored on %s", a.param, a.node)
	}
}

func isUnrecognizedParam(err error) bool {
	var cmdErr mongo.CommandError
	if !errors.As(err, &cmdErr) {
		return false
	}
	// InvalidOptions
	return cmdErr.Code == 72 && strings.Contains(cmdErr.Message, "unrecognized parameter")
}

type ts struct {
	col  string
	cnt  uint64
	done chan struct{}

	m *pbmt.Mongo
}

func (c *Cluster) newTS(col string) (*ts, error) {
	err := c.mongos.CreateTS(col)
	if err != nil {
		return nil, err
	}
	return &ts{
		col:  col,
		done: make(chan struct{}),
		m:    c.mongos,
	}, nil
}

func (t *ts) gen() {
	go func() {
		for {
			select {
			case <-t.done:
				return
			default:
			}

			err := t.m.InsertTS(t.col)
			if err != nil {
				log.Fatalf("Error: insert timeseries into %s: %v", t.col, err)
			}
			t.cnt++
		}
	}()
}

func (t *ts) count() uint64 {
	return t.cnt
}

func (t *ts) stop() {
	t.done <- struct{}{}
}
