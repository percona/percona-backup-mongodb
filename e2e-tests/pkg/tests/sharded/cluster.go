package sharded

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	pbmt "github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/storage"

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

	cfg ClusterConf
}

type ClusterConf struct {
	Configsrv       string
	Mongos          string
	Shards          map[string]string
	DockerSocket    string
	PbmContainer    string
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
		cfg:      cfg,
	}
	for name, uri := range cfg.Shards {
		c.shards[name] = mgoConn(ctx, uri)
	}

	pbmObj, err := pbm.NewCtl(c.ctx, cfg.DockerSocket, cfg.PbmContainer)
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

func (c *Cluster) Reconnect() {
	ctx := context.Background()
	c.mongos = mgoConn(ctx, c.cfg.Mongos)
	c.mongopbm = pbmConn(ctx, c.cfg.Configsrv)

	for name, uri := range c.cfg.Shards {
		c.shards[name] = mgoConn(ctx, uri)
	}
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
		Type: pbmt.CmdResync,
	},
		time.Minute*5,
	)
	if err != nil {
		log.Fatalf("waiting for the store resync: %v", err)
	}

	time.Sleep(time.Second * 6) // give time to refresh agent-checks
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

func (c *Cluster) LogicalRestore(bcpName string) {
	c.LogicalRestoreWithParams(bcpName, []string{})
}

func (c *Cluster) LogicalRestoreWithParams(bcpName string, options []string) {

	log.Println("restoring the backup")
	_, err := c.pbm.Restore(bcpName, options)
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

func (c *Cluster) PhysicalRestore(bcpName string) {
	c.PhysicalRestoreWithParams(bcpName, []string{})
}

func (c *Cluster) PhysicalRestoreWithParams(bcpName string, options []string) {
	log.Println("restoring the backup")
	name, err := c.pbm.Restore(bcpName, options)
	if err != nil {
		log.Fatalln("restoring the backup:", err)
	}

	log.Println("waiting for the restore", name)
	err = c.waitPhyRestore(name, time.Minute*25)
	if err != nil {
		log.Fatalln("check backup restore:", err)
	}
	// just wait so the all data gonna be written (aknowleged) before the next steps
	time.Sleep(time.Second * 1)

	// sharded cluster, hence have a mongos
	if c.confsrv == "cfg" {
		log.Println("stopping mongos")
		err = c.docker.StopContainers([]string{"com.percona.pbm.app=mongos"})
		if err != nil {
			log.Fatalln("stop mongos:", err)
		}
	}

	log.Println("restarting the culster")
	err = c.docker.StartContainers([]string{"com.percona.pbm.app=mongod"})
	if err != nil {
		log.Fatalln("restart mongod:", err)
	}

	time.Sleep(time.Second * 5)

	if c.confsrv == "cfg" {
		log.Println("starting mongos")
		err = c.docker.StartContainers([]string{"com.percona.pbm.app=mongos"})
		if err != nil {
			log.Fatalln("start mongos:", err)
		}
	}

	log.Println("restarting agents")
	err = c.docker.StartAgentContainers([]string{"com.percona.pbm.app=agent"})
	if err != nil {
		log.Fatalln("restart agents:", err)
	}

	// Give time for agents to report its availability status
	// after the restart
	time.Sleep(time.Second * 7)

	log.Println("resync")
	err = c.pbm.Resync()
	if err != nil {
		log.Fatalln("resync:", err)
	}

	c.Reconnect()

	err = c.mongopbm.WaitOp(&pbmt.LockHeader{
		Type: pbmt.CmdResync,
	},
		time.Minute*5,
	)
	if err != nil {
		log.Fatalf("waiting for resync: %v", err)
	}

	time.Sleep(time.Second * 7)

	log.Printf("restore finished '%s'\n", bcpName)
}

func (c *Cluster) waitPhyRestore(name string, waitFor time.Duration) error {
	stg, err := c.mongopbm.Storage()
	if err != nil {
		return errors.Wrap(err, "get storage")
	}

	fname := fmt.Sprintf("%s/%s.json", pbmt.PhysRestoresDir, name)
	log.Println("checking", fname)

	tmr := time.NewTimer(waitFor)
	tkr := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-tmr.C:
			list, err := c.pbm.RunCmd("pbm", "status")
			if err != nil {
				return errors.Wrap(err, "timeout reached. get status")
			}
			return errors.Errorf("timeout reached. status:\n%s", list)
		case <-tkr.C:
			rmeta, err := getRestoreMetaStg(fname, stg)
			if errors.Is(err, pbmt.ErrNotFound) {
				continue
			}
			if err != nil {
				return errors.Wrap(err, "get restore metadata")
			}

			switch rmeta.Status {
			case pbmt.StatusDone:
				return nil
			case pbmt.StatusError:
				return errors.Errorf("restore failed with: %s", rmeta.Error)
			}
		}
	}
}

func getRestoreMetaStg(name string, stg storage.Storage) (*pbmt.RestoreMeta, error) {
	_, err := stg.FileStat(name)
	if err == storage.ErrNotExist {
		return nil, pbmt.ErrNotFound
	}
	if err != nil {
		return nil, errors.Wrap(err, "get stat")
	}

	src, err := stg.SourceReader(name)
	if err != nil {
		return nil, errors.Wrapf(err, "get file %s", name)
	}

	rmeta := new(pbmt.RestoreMeta)
	err = json.NewDecoder(src).Decode(rmeta)
	if err != nil {
		return nil, errors.Wrapf(err, "decode meta %s", name)
	}

	return rmeta, nil
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

func (c *Cluster) LogicalBackup() string {
	return c.backup(pbmt.LogicalBackup)
}

func (c *Cluster) PhysicalBackup() string {
	return c.backup(pbmt.PhysicalBackup)
}

func (c *Cluster) ReplayOplog(a, b time.Time) {
	log.Printf("replay oplog from %v to %v", a, b)
	if err := c.pbm.ReplayOplog(a, b); err != nil {
		log.Fatalln("restore:", err)
	}

	log.Println("waiting for the oplog replay")
	if err := c.pbm.CheckOplogReplay(a, b, 25*time.Minute); err != nil {
		log.Fatalln("check restore:", err)
	}

	// just wait so the all data gonna be written (aknowleged) before the next steps
	time.Sleep(time.Second)
	log.Printf("replay oplog from %v to %v finished", a, b)
}

func (c *Cluster) backup(typ pbmt.BackupType, opts ...string) string {
	log.Println("starting backup")
	bcpName, err := c.pbm.Backup(typ, opts...)
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
				log.Fatalf("%s: hashes don't match. before %s now %s", name, hashes1[name]["_all_"], h["_all_"])
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
				// to be sure the lock is released
				time.Sleep(time.Second * 3)
				return nil
			case pbmt.StatusError:
				return m.Error()
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
