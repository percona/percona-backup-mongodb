package sharded

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/pbm"
)

func (c *Cluster) PITRbasic() {
	bcpName := c.Backup()

	c.pitrOn()
	defer c.pitrOff()

	rand.Seed(time.Now().UnixNano())

	type shrd struct {
		name   string
		cnt    *pcounter
		cancel context.CancelFunc
	}
	counters := make(map[string]shrd)
	for name, cn := range c.shards {
		c.bcheckClear(name, cn)
		pcc := newpcounter(name, cn)
		ctx, cancel := context.WithCancel(context.TODO())
		go pcc.write(ctx, time.Millisecond*10*time.Duration(rand.Int63n(49)+1))
		counters[name] = shrd{
			cnt:    pcc,
			cancel: cancel,
		}
	}

	c.BackupWaitDone(bcpName)

	ds := time.Second * 30 * time.Duration(rand.Int63n(5)+2)
	log.Printf("Generating data for %v", ds)
	time.Sleep(ds)

	log.Println("Get reference time")
	refc := make(map[string]*pbm.Counter)
	var lastt primitive.Timestamp
	for name, c := range counters {
		cc := c.cnt.current()
		log.Printf("\tshard %s: %d [%v] | %v", name, cc.WriteTime.T, time.Unix(int64(cc.WriteTime.T), 0), cc)
		if cc.WriteTime.T > lastt.T {
			lastt = cc.WriteTime
		}
		refc[name] = cc
	}

	ds = time.Second * 30 * time.Duration(rand.Int63n(5)+2)
	log.Printf("Generating data for %v", ds)
	time.Sleep(ds)

	for _, c := range counters {
		c.cancel()
	}

	c.pitrOff()
	log.Println("Turning pitr off")
	log.Println("Sleep")
	time.Sleep(time.Second * 60)

	for name, shard := range c.shards {
		c.bcheckClear(name, shard)
	}

	// -1 sec back since we are PITR restore done up to < time (not <=)
	c.PITRestore(time.Unix(int64(lastt.T), 0).Add(time.Second * -1))

	for name, shard := range c.shards {
		c.bcheckCheck(name, shard, &counters[name].cnt.data, lastt)
	}
}

func (c *Cluster) pitrOn() {
	err := c.pbm.PITRon()
	if err != nil {
		log.Fatalf("ERROR: turn PITR on: %v\n", err)
	}
}

func (c *Cluster) pitrOff() {
	err := c.pbm.PITRoff()
	if err != nil {
		log.Fatalf("ERROR: turn PITR off: %v\n", err)
	}
}

type pcounter struct {
	mx    sync.Mutex
	data  []pbm.Counter
	curr  *pbm.Counter
	shard string
	cn    *pbm.Mongo
}

func newpcounter(shard string, cn *pbm.Mongo) *pcounter {
	return &pcounter{
		shard: shard,
		cn:    cn,
	}
}

func (pc *pcounter) write(ctx context.Context, t time.Duration) {
	tk := time.NewTicker(t)
	defer tk.Stop()

	for cnt := 0; ; cnt++ {
		select {
		case <-tk.C:
			td, err := pc.cn.WriteCounter(cnt)
			if err != nil {
				log.Fatalln("ERROR:", pc.shard, "write test counter:", err)
			}

			td.WriteTime, err = pc.cn.GetLastWrite()
			if err != nil {
				log.Fatalln("ERROR:", pc.shard, "get cluster last write time:", err)
			}
			pc.data = append(pc.data, *td)
			pc.mx.Lock()
			pc.curr = td
			pc.mx.Unlock()
		case <-ctx.Done():
			return
		}
	}
}

func (pc *pcounter) current() *pbm.Counter {
	pc.mx.Lock()
	defer pc.mx.Unlock()
	return pc.curr
}
