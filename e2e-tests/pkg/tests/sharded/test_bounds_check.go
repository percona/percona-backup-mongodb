package sharded

import (
	"context"
	"log"
	"math/rand"
	"time"

	pbmt "github.com/percona/percona-backup-mongodb/pbm"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/pbm"
)

type scounter struct {
	data   <-chan *[]pbm.Counter
	cancel context.CancelFunc
}

func (c *Cluster) BackupBoundsCheck(typ pbmt.BackupType) {
	backup := c.LogicalBackup
	restore := c.LogicalRestore
	if typ == pbmt.PhysicalBackup {
		backup = c.PhysicalBackup
		restore = c.PhysicalRestore
	}

	rand.Seed(time.Now().UnixNano())
	counters := make(map[string]scounter)
	for name, shard := range c.shards {
		c.bcheckClear(name, shard)
		dt, cancel := c.bcheckWrite(name, shard, time.Millisecond*10*time.Duration(rand.Int63n(49)+1))
		counters[name] = scounter{
			data:   dt,
			cancel: cancel,
		}
	}

	bcpName := backup()

	c.BackupWaitDone(bcpName)
	time.Sleep(time.Second * 1)

	for _, c := range counters {
		c.cancel()
	}

	bcpMeta, err := c.mongopbm.GetBackupMeta(bcpName)
	if err != nil {
		log.Fatalf("ERROR: get backup '%s' metadata: %v\n", bcpName, err)
	}
	// fmt.Println("BCP_LWT:", bcpMeta.LastWriteTS)

	c.DeleteBallast()
	for name, shard := range c.shards {
		c.bcheckClear(name, shard)
	}

	restore(bcpName)

	for name, shard := range c.shards {
		c.bcheckCheck(name, shard, <-counters[name].data, bcpMeta.LastWriteTS)
	}
}

func (c *Cluster) bcheckClear(name string, shard *pbm.Mongo) {
	log.Println(name, "reseting counters")
	dcnt, err := shard.ResetCounters()
	if err != nil {
		log.Fatalln("ERROR:", name, "reseting counters:", err)
	}
	log.Println(name, "deleted counters:", dcnt)
}

func (c *Cluster) bcheckWrite(name string, shard *pbm.Mongo, t time.Duration) (<-chan *[]pbm.Counter, context.CancelFunc) {
	var data []pbm.Counter
	ctx, cancel := context.WithCancel(c.ctx)
	dt := make(chan *[]pbm.Counter)
	go func() {
		log.Println(name, "writing counters")
		tk := time.NewTicker(t)
		defer tk.Stop()
		cnt := 0
		for {
			select {
			case <-tk.C:
				td, err := shard.WriteCounter(cnt)
				if err != nil {
					log.Fatalln("ERROR:", name, "write test counter:", err)
				}

				td.WriteTime, err = shard.GetLastWrite()
				if err != nil {
					log.Fatalln("ERROR:", name, "get cluster last write time:", err)
				}

				// fmt.Println("->", cnt, td.WriteTime)
				data = append(data, *td)
				cnt++
			case <-ctx.Done():
				log.Println(name, "writing counters finished")
				dt <- &data
				return
			}
		}
	}()

	return dt, cancel
}

func (c *Cluster) bcheckCheck(name string, shard *pbm.Mongo, data *[]pbm.Counter, bcpLastWrite primitive.Timestamp) {
	log.Println(name, "getting restored counters")
	restored, err := shard.GetCounters()
	if err != nil {
		log.Fatalln("ERROR: ", name, "get data:", err)
	}

	log.Println(name, "checking restored counters")
	var lastc pbm.Counter
	for i, d := range *data {
		if primitive.CompareTimestamp(d.WriteTime, bcpLastWrite) <= 0 {
			if len(restored) <= i {
				log.Fatalf("ERROR: %s no record #%d/%d in restored (%d) | last: %v\n", name, i, d.Count, len(restored), lastc)
			}
			r := restored[i]
			if d.Count != r.Count {
				log.Fatalf("ERROR: %s unmatched backuped %v and restored %v. Bcp last write: %v\n", name, d, r, bcpLastWrite)
			}
		} else if i < len(restored) {
			r := restored[i]
			log.Fatalf("ERROR: %s data %v shouldn't be restored. Cmp to: %v. Bcp last write: %v\n", name, r, d, bcpLastWrite)
		}

		lastc = d
	}
}
