package sharded

import (
	"context"
	"log"
	"time"

	tpbm "github.com/percona/percona-backup-mongodb/e2e-tests/pkg/pbm"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func (c *Cluster) BackupAndRestore() {
	checkData := c.DataChecker()

	bcpName := c.Backup()
	c.BackupWaitDone(bcpName)
	c.DeleteBallast()

	c.Restore(bcpName)
	checkData()
}

func (c *Cluster) BackupBoundsCheck() {
	bcpName := c.Backup()

	log.Println("reseting counters")
	dcnt, err := c.mongo01.ResetCounters()
	if err != nil {
		log.Fatalln("reseting counters:", err)
	}
	log.Println("deleted counters:", dcnt)

	var data []tpbm.Counter
	ctx, cancel := context.WithCancel(c.ctx)
	go func() {
		log.Println("writing counters")
		tk := time.NewTicker(time.Millisecond * 100)
		defer tk.Stop()
		cnt := 0
		for {
			select {
			case <-tk.C:
				td, err := c.mongo01.WriteCounter(cnt)
				if err != nil {
					log.Fatalf("write test data for backup '%s': %v\n", bcpName, err)
				}

				td.WriteTime, err = c.mongo01.GetLastWrite()
				if err != nil {
					log.Fatalf("get cluster last write time: %v\n", err)
				}

				// fmt.Println("->", cnt, td.WriteTime)
				data = append(data, *td)
				cnt++
			case <-ctx.Done():
				log.Println("writing counters finished")
				return
			}
		}
	}()

	c.BackupWaitDone(bcpName)
	time.Sleep(time.Second * 1)
	cancel()

	bcpMeta, err := c.mongopbm.GetBackupMeta(bcpName)
	if err != nil {
		log.Fatalf("get backup '%s' metadata: %v\n", bcpName, err)
	}
	// fmt.Println("BCP_LWT:", bcpMeta.LastWriteTS)

	c.DeleteBallast()
	log.Println("reseting counters")
	dcnt, err = c.mongo01.ResetCounters()
	if err != nil {
		log.Fatalln("reseting counters:", err)
	}
	log.Println("deleted counters:", dcnt)

	c.Restore(bcpName)

	log.Println("getting restored counters")
	restored, err := c.mongo01.GetCounters()
	if err != nil {
		log.Fatalln("get data:", err)
	}

	log.Println("checking restored counters")

	for i, d := range data {
		if primitive.CompareTimestamp(d.WriteTime, bcpMeta.LastWriteTS) <= 0 {
			if len(restored) <= i {
				log.Fatalf("no record #%d/%d in restored (%d)\n", i, d.Count, len(restored))
			}
			r := restored[i]
			if d.Count != r.Count {
				log.Fatalf("unmatched backuped %#v and restored %#v\n", d, r)
			}
		} else {
			if i < len(restored) {
				r := restored[i]
				log.Fatalf("data %#v souldn't be restored\n", r)
			}
		}
	}
}
