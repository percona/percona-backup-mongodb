package sharded

import (
	"context"
	"log"
	"time"

	pbmt "github.com/percona/percona-backup-mongodb/pbm"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/pbm"
)

type Cluster struct {
	ctx context.Context

	pbm    *pbm.Ctl
	docker *pbm.Docker

	mongos   *pbm.Mongo
	shards   map[string]*pbm.Mongo
	mongopbm *pbm.MongoPBM
	confsrv  string
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
		confsrv:  cfg.ConfigsrvRsName,
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

func (c *Cluster) PITRestoreCT(t primitive.Timestamp) {
	log.Printf("restoring to the point-in-time %v", t)
	err := c.pbm.PITRestoreClusterTime(t.T, t.I)
	if err != nil {
		log.Fatalln("restore:", err)
	}

	log.Println("waiting for the restore")
	err = c.pbm.CheckPITRestore(time.Unix(int64(t.T), 0), time.Minute*25)
	if err != nil {
		log.Fatalln("check restore:", err)
	}
	// just wait so the all data gonna be written (aknowleged) before the next steps
	time.Sleep(time.Second * 1)
	log.Printf("restore to the point-in-time '%v' finished", t)
}

func (c *Cluster) PITRestore(t time.Time) {
	log.Printf("restoring to the point-in-time %v", t)
	err := c.pbm.PITRestore(t)
	if err != nil {
		log.Fatalln("restore:", err)
	}

	log.Println("waiting for the restore")
	err = c.pbm.CheckPITRestore(t, time.Minute*25)
	if err != nil {
		log.Fatalln("check restore:", err)
	}
	// just wait so the all data gonna be written (aknowleged) before the next steps
	time.Sleep(time.Second * 1)
	log.Printf("restore to the point-in-time '%v' finished", t)
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
	err := c.checkBackup(bcpName, time.Minute*25)
	if err != nil {
		log.Fatalln("check backup state:", err)
	}

	// locks being released NOT immediately after the backup succeed
	// see https://github.com/percona/percona-backup-mongodb/blob/v1.1.3/agent/agent.go#L128-L143
	needToWait := pbmt.WaitBackupStart + time.Second - time.Since(ts)
	if needToWait > 0 {
		log.Printf("waiting for the lock to be released for %s", needToWait)
		time.Sleep(needToWait)
	}
	log.Printf("backup finished '%s'\n", bcpName)
}

func (c *Cluster) SetBallastData(amount int64) {
	log.Println("set ballast data to", amount)
	cnt, err := c.mongos.SetBallast(amount)
	if err != nil {
		log.Fatalln("generating ballast:", err)
	}
	log.Println("ballast data:", cnt)
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

// Flush removes all backups, restores and PITR chunks metadata from the PBM db
func (c *Cluster) Flush() error {
	cols := []string{
		pbmt.BcpCollection,
		pbmt.PITRChunksCollection,
		pbmt.RestoresCollection,
	}
	for _, cl := range cols {
		_, err := c.mongopbm.Conn().Database(pbmt.DB).Collection(cl).DeleteMany(context.Background(), bson.M{})
		if err != nil {
			return errors.Wrapf(err, "delete many from %s", cl)
		}
	}

	return nil
}

func (c *Cluster) FlushStorage() error {
	stg, err := c.mongopbm.Storage()
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	fls, err := stg.List("", "")
	if err != nil {
		return errors.Wrap(err, "get files list")
	}

	for _, f := range fls {
		err = stg.Delete(f.Name)
		if err != nil {
			log.Println("Warning: unable to delete", f.Name)
		}
	}

	return nil
}

func (c *Cluster) checkBackup(bcpName string, waitFor time.Duration) error {
	tmr := time.NewTimer(waitFor)
	tkr := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-tmr.C:
			sts, err := c.pbm.RunCmd("pbm", "status")
			if err != nil {
				return errors.Wrap(err, "timeout reached. pbm status")
			}
			return errors.Errorf("timeout reached. pbm status:\n%s", sts)
		case <-tkr.C:
			m, err := c.mongopbm.GetBackupMeta(bcpName)
			if errors.Is(err, pbmt.ErrNotFound) {
				continue
			}
			if err != nil {
				return errors.Wrap(err, "get backup meta")
			}
			switch m.Status {
			case pbmt.StatusDone:
				return nil
			case pbmt.StatusError:
				return errors.New(m.Error)
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
