package sharded

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	tpbm "github.com/percona/percona-backup-mongodb/e2e-tests/pkg/pbm"
)

func (c *Cluster) BackupAndRestore() {
	checkData := c.DataChecker()

	bcpName := c.Backup()
	c.BackupWaitDone(bcpName)
	c.DeleteData()

	c.Restore(bcpName)
	checkData()
}

func (c *Cluster) BackupCheckBounds() {
	bcpName := c.Backup()

	log.Println("reseting counters")
	dcnt, err := c.mongos.ResetData()
	if err != nil {
		log.Fatalln("reseting counters:", err)
	}
	log.Println("deleted counters:", dcnt)

	var data []tpbm.TestData
	ctx, cancel := context.WithCancel(c.ctx)
	go func() {
		log.Println("writing counters")
		tk := time.NewTicker(time.Millisecond * 100)
		defer tk.Stop()
		cnt := 0
		for {
			select {
			case <-tk.C:
				metas, err := c.mongopbm.GetBackupMeta(bcpName)
				if err != nil {
					log.Fatalf("get backup '%s' metadata: %v\n", bcpName, err)
				}

				ctimeNow, err := c.mongopbm.GetClusterTime()
				if err != nil {
					log.Fatalf("get cluster time: %v\n", err)
				}

				td, err := c.mongos.WriteData(cnt)
				if err != nil {
					log.Fatalf("write test dat  for backup '%s': %v\n", bcpName, err)
				}

				td.Status = tpbm.InBcpYES
				if metas.LastWriteTS.T > 1 && primitive.CompareTimestamp(ctimeNow, metas.LastWriteTS) == 1 { // ctimeNow < LastWriteTS
					td.Status = tpbm.InBcpNO
				}

				// metae, err := c.mongopbm.GetBackupMeta(bcpName)
				// if err != nil {
				// 	log.Fatalf("get backup '%s' metadata: %v\n", bcpName, err)
				// }

				// if metae.Status != pbm.StatusDumpDone && metae.Status != pbm.StatusDone { //run
				// 	td.Status = tpbm.InBcpYES
				// } else if metas.Status == pbm.StatusDumpDone || metas.Status == pbm.StatusDone {
				// 	td.Status = tpbm.InBcpNO
				// }
				fmt.Println("->", cnt, metas.Status, td.Status, metas.LastWriteTS, ctimeNow) //, td.TS.Unix())

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

	// //------------------
	// //------------------
	// gdc, err := c.mongos.GetData()
	// if err != nil {
	// 	log.Fatalln("get gdc data:", err)
	// }
	// log.Println("-------Inserted counters---------")
	// for i, d := range gdc {
	// 	fmt.Println(i, d.Count, d.Status)
	// }
	// log.Println("-------!Inserted counters---------")
	// //------------------
	// //------------------

	c.DeleteData()
	log.Println("reseting counters")
	dcnt, err = c.mongos.ResetData()
	if err != nil {
		log.Fatalln("reseting counters:", err)
	}
	log.Println("deleted counters:", dcnt)

	c.Restore(bcpName)

	log.Println("getting restored counters")
	restored, err := c.mongos.GetData()
	if err != nil {
		log.Fatalln("get data:", err)
	}

	log.Println("checking restored counters")
	// for i, d := range data {
	// 	fmt.Println(i, d.Count, d.Status)
	// }
	// log.Println("-----------------")
	// for i, d := range restored {
	// 	fmt.Println(i, d.Count, d.Status)
	// }

	// var checked int
	for i, d := range data {
		switch d.Status {
		case tpbm.InBcpYES:
			if len(restored) <= i {
				log.Fatalf("no record #%d/%d in restored (%d)\n", i, d.Count, len(restored))
			}
			r := restored[i]
			if d.Count != r.Count {
				log.Fatalf("unmatched backuped %#v and restored %#v\n", d, r)
			}
			// checked++
		case tpbm.InBcpUndef:
			if i < len(restored) {
				r := restored[i]
				log.Printf("borderline data %#v\n", r)
				// checked++
			}
		case tpbm.InBcpNO:
			if i < len(restored) {
				r := restored[i]
				log.Fatalf("data %#v souldn't be restored\n", r)
			}
		}
	}

	// if checked != len(restored) {
	// 	log.Fatalf("restored data has leftovers %#v\n", restored[checked:])
	// }
}
