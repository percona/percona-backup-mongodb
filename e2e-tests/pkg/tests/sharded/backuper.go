package sharded

import (
	"log"
	"math/rand"
	"time"

	"github.com/percona/percona-backup-mongodb/e2e-tests/pkg/pbm"
)

type Backuper interface {
	Backup(done chan<- struct{})
	Restore()
}

type Snapshot struct {
	bcpName string
	c       *Cluster
}

func NewSnapshot(c *Cluster) *Snapshot {
	return &Snapshot{c: c}
}

func (s *Snapshot) Backup(done chan<- struct{}) {
	s.bcpName = s.c.Backup()
	s.c.BackupWaitDone(s.bcpName)
	time.Sleep(time.Second * 1)
	if done != nil {
		done <- struct{}{}
	}
}

func (s *Snapshot) Restore() {
	s.c.Restore(s.bcpName)
}

type Pitr struct {
	pointT time.Time
	c      *Cluster
}

func NewPitr(c *Cluster) *Pitr {
	return &Pitr{c: c}
}

func (p *Pitr) Backup(done chan<- struct{}) {
	rand.Seed(time.Now().UnixNano())

	bcpName := p.c.Backup()
	p.c.pitrOn()
	p.c.BackupWaitDone(bcpName)

	ds := time.Second * 30 * time.Duration(rand.Int63n(5)+2)
	log.Printf("PITR slicing for %v", ds)
	time.Sleep(ds)

	var cn *pbm.Mongo
	for _, cn = range p.c.shards {
		break
	}
	lw, err := cn.GetLastWrite()
	if err != nil {
		log.Fatalln("ERROR: get cluster last write time:", err)
	}
	p.pointT = time.Unix(int64(lw.T), 0)

	if done != nil {
		done <- struct{}{}
	}
}

func (p *Pitr) Restore() {
	p.c.pitrOff()
	p.c.PITRestore(p.pointT)
}
