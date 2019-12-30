package sharded

import (
	"context"
	"log"
	"time"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/pbm"
)

const (
	cnMongos    = "mongodb://dba:test1234@mongos:27017/"
	cnConfigsrv = "mongodb://dba:test1234@cfg01:27017/"
	cnRS01      = "mongodb://dba:test1234@rs101:27017/"
	cnRS02      = "mongodb://dba:test1234@rs201:27017/"

	dockerSocket = "unix:///var/run/docker.sock"
)

type Cluster struct {
	pbm *pbm.Ctl

	mongos  *pbm.Mongo
	mongo01 *pbm.Mongo
	mongo02 *pbm.Mongo

	mongopbm *pbm.MongoPBM

	ctx context.Context
}

func New() *Cluster {
	ctx := context.Background()
	c := &Cluster{
		ctx:      ctx,
		mongos:   mgoConn(ctx, cnMongos),
		mongo01:  mgoConn(ctx, cnRS01),
		mongo02:  mgoConn(ctx, cnRS02),
		mongopbm: pbmConn(ctx, cnConfigsrv),
	}

	pbmObj, err := pbm.NewCtl(c.ctx, dockerSocket)
	if err != nil {
		log.Fatalln("connect to mongo:", err)
	}
	log.Println("connected to pbm")

	log.Println("apply config")
	err = pbmObj.ApplyConfig()
	if err != nil {
		l, _ := pbmObj.ContainerLogs()
		log.Fatalf("apply config: %v\nconatiner logs: %s\n", err, l)
	}

	c.pbm = pbmObj
	return c
}

func (c *Cluster) DeleteData() {
	log.Println("deleteing data")
	deleted, err := c.mongos.ResetBallast()
	if err != nil {
		log.Fatalln("deleting data:", err)
	}
	log.Printf("deleted %d documents", deleted)
}

func (c *Cluster) Restore(bcpName string) {
	log.Println("restoring the backup")
	err := c.pbm.Restore(bcpName)
	if err != nil {
		log.Fatalln("restoring the backup:", err)
	}

	log.Println("waiting for the restore")
	err = c.mongopbm.CheckRestore(bcpName, time.Minute*15)
	if err != nil {
		log.Fatalln("check backup restore:", err)
	}
	// just wait so the all data gonna be written (aknowleged) before the next steps
	time.Sleep(time.Second * 1)
	log.Printf("restore finished '%s'\n", bcpName)
}

func (c *Cluster) Backup() string {
	log.Println("starting the backup")
	bcpName, err := c.pbm.Backup()
	if err != nil {
		l, _ := c.pbm.ContainerLogs()
		log.Fatalf("starting backup: %v\nconatiner logs: %s\n", err, l)
	}
	log.Printf("backup started '%s'\n", bcpName)

	return bcpName
}

func (c *Cluster) BackupWaitDone(bcpName string) {
	log.Println("waiting for the backup")
	err := c.pbm.CheckBackup(bcpName, time.Second*240)
	if err != nil {
		log.Fatalln("check backup state:", err)
	}
	log.Printf("backup finished '%s'\n", bcpName)
}

func (c *Cluster) GenerateBallastData(amount int) {
	log.Println("genrating ballast data")
	err := c.mongos.Fill(amount)
	if err != nil {
		log.Fatalln("genrating ballast:", err)
	}
	log.Println("ballast data generated")
}

func (c *Cluster) DataChecker() (check func()) {
	dbhash01, err := c.mongo01.Hashes()
	if err != nil {
		log.Fatalln("get db hashes rs01:", err)
	}
	log.Println("current rs01 db hash", dbhash01["_all_"])

	dbhash02, err := c.mongo02.Hashes()
	if err != nil {
		log.Fatalln("get db hashes rs02:", err)
	}
	log.Println("current rs02 db hash", dbhash02["_all_"])

	fnc := func() {
		log.Println("Checking restored backup")

		dbrhash01, err := c.mongo01.Hashes()
		if err != nil {
			log.Fatalln("get db hashes rs01:", err)
		}
		log.Println("current rs01 db hash", dbhash01["_all_"])

		dbrhash02, err := c.mongo02.Hashes()
		if err != nil {
			log.Fatalln("get db hashes rs02:", err)
		}
		log.Println("current rs02 db hash", dbhash02["_all_"])

		if rh, ok := dbrhash01["_all_"]; !ok || rh != dbhash01["_all_"] {
			log.Fatalf("rs01 hashes not equal: %s != %s\n", rh, dbhash01["_all_"])
		}

		if rh, ok := dbrhash02["_all_"]; !ok || rh != dbhash02["_all_"] {
			log.Fatalf("rs02 hashes not equal: %s != %s\n", rh, dbhash02["_all_"])
		}
	}

	return fnc
}

func mgoConn(ctx context.Context, uri string) *pbm.Mongo {
	m, err := pbm.NewMongo(ctx, uri)
	if err != nil {
		log.Fatalln("connect to mongo:", err)
	}
	log.Println("connected to", uri)
	return m
}
func pbmConn(ctx context.Context, uri string) *pbm.MongoPBM {
	m, err := pbm.NewMongoPBM(ctx, uri)
	if err != nil {
		log.Fatalln("connect to mongo:", err)
	}
	log.Println("connected to", uri)
	return m
}
