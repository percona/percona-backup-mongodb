package sharded

import (
	"context"
	"log"
	"time"

	pbmt "github.com/percona/percona-backup-mongodb/pbm"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/pbm"
)

type Cluster struct {
	ctx context.Context

	pbm    *pbm.Ctl
	docker *pbm.Docker

	mongos   *pbm.Mongo
	shards   map[string]*pbm.Mongo
	mongopbm *pbm.MongoPBM
}

type ClusterConf struct {
	Configsrv       string
	Mongos          string
	Shards          map[string]string
	DockerSocket    string
	ConfigsrvRsName string
}

func New(cfg ClusterConf) *Cluster {
	ctx := context.Background()
	c := &Cluster{
		ctx:      ctx,
		mongos:   mgoConn(ctx, cfg.Mongos),
		mongopbm: pbmConn(ctx, cfg.Configsrv),
		shards:   make(map[string]*pbm.Mongo),
	}
	for name, uri := range cfg.Shards {
		c.shards[name] = mgoConn(ctx, uri)
	}

	pbmObj, err := pbm.NewCtl(c.ctx, cfg.DockerSocket)
	if err != nil {
		log.Fatalln("connect to mongo:", err)
	}
	log.Println("connected to pbm")

	c.pbm = pbmObj

	c.docker, err = pbm.NewDocker(c.ctx, cfg.DockerSocket)
	if err != nil {
		log.Fatalln("connect to docker:", err)
	}
	log.Println("connected to docker")

	return c
}

func (c *Cluster) ApplyConfig(file string) {
	log.Println("apply config")
	err := c.pbm.ApplyConfig(file)
	if err != nil {
		l, _ := c.pbm.ContainerLogs()
		log.Fatalf("apply config: %v\nconatiner logs: %s\n", err, l)
	}

	log.Println("waiting for the new storage to resync")
	err = c.mongopbm.WaitOp(&pbmt.LockHeader{
		Type: pbmt.CmdResyncBackupList,
	},
		time.Minute*5,
	)
	if err != nil {
		log.Fatalf("waiting for the store resync: %v", err)
	}
}

func (c *Cluster) ServerVersion() string {
	v, err := c.mongos.ServerVersion()
	if err != nil {
		log.Fatalln("Get server version:", err)
	}
	return v
}

func (c *Cluster) DeleteBallast() {
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
	err = c.pbm.CheckRestore(bcpName, time.Minute*25)
	if err != nil {
		log.Fatalln("check backup restore:", err)
	}
	// just wait so the all data gonna be written (aknowleged) before the next steps
	time.Sleep(time.Second * 1)
	log.Printf("restore finished '%s'\n", bcpName)
}

func (c *Cluster) Backup() string {
	log.Println("starting backup")
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
	ts := time.Now()
	err := c.pbm.CheckBackup(bcpName, time.Minute*25)
	if err != nil {
		log.Fatalln("check backup state:", err)
	}

	// locks being released NOT immediately after the backup succeed
	// see https://github.com/percona/percona-backup-mongodb/blob/v1.1.3/agent/agent.go#L128-L143
	needToWait := pbmt.WaitActionStart + time.Second - time.Since(ts)
	if needToWait > 0 {
		log.Printf("waiting for the lock to be released for %s", needToWait)
		time.Sleep(needToWait)
	}
	log.Printf("backup finished '%s'\n", bcpName)
}

func (c *Cluster) GenerateBallastData(amount int) {
	log.Println("genrating ballast data")
	err := c.mongos.GenBallast(amount)
	if err != nil {
		log.Fatalln("genrating ballast:", err)
	}
	log.Println("ballast data generated")
}

func (c *Cluster) DataChecker() (check func()) {
	hashes1 := make(map[string]map[string]string)
	for name, s := range c.shards {
		h, err := s.DBhashes()
		if err != nil {
			log.Fatalf("get db hashes %s: %v\n", name, err)
		}
		log.Printf("current %s db hash %s\n", name, h["_all_"])
		hashes1[name] = h
	}

	return func() {
		log.Println("Checking restored backup")

		for name, s := range c.shards {
			h, err := s.DBhashes()
			if err != nil {
				log.Fatalf("get db hashes %s: %v\n", name, err)
			}
			if hashes1[name]["_all_"] != h["_all_"] {
				log.Fatalf("%s: hashes doesn't match. before %s now %s", name, hashes1[name]["_all_"], h["_all_"])
			}
		}
	}
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
