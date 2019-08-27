package agent

import (
	"log"
	"math/rand"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Agent struct {
	pbm  *pbm.PBM
	node *pbm.Node
}

func New(pbmConn *mongo.Client) *Agent {
	return &Agent{
		pbm: pbm.New(pbmConn),
	}
}

func (a *Agent) AddNode(cn *mongo.Client, curi string) {
	a.node = pbm.NewNode("node0", cn, curi)
}

// Start starts listening the commands stream.
func (a *Agent) Start() error {
	c, cerr, err := a.pbm.ListenCmd()
	if err != nil {
		return errors.Wrap(err, "listen commands stream")
	}

	for {
		select {
		case cmd := <-c:
			switch cmd.Cmd {
			case pbm.CmdBackup:
				a.Backup(cmd.Backup)
			case pbm.CmdRestore:
			}
		case err := <-cerr:
			log.Println(err)
		}
	}
}

// Backup starts backup
func (a *Agent) Backup(bcp pbm.BackupCmd) {
	q, err := backup.NodeQualify(bcp, a.node)
	if err != nil {
		log.Println("[ERROR] backup: unable to check node:", err)
		return
	}

	if !q {
		log.Println("node not qualified for backup")
		return
	}

	nodeInfo, err := a.node.GetIsMaster()
	if err != nil {
		log.Println("[ERROR] backup: get node isMaster data:", err)
		return
	}

	lock := pbm.Lock{
		Type:       "backup",
		Replset:    nodeInfo.SetName,
		Node:       nodeInfo.Me,
		BackupName: bcp.Name,
	}

	// have wait random time (1 to 100 ms) before acquiring lock
	// otherwise all angent could aquire own locks
	time.Sleep(time.Duration(rand.Int63n(1e2)) * time.Millisecond)
	// TODO: check if lock from "another" backup and notify user
	got, err := a.pbm.AcquireLock(lock)
	if err != nil {
		log.Println("[ERROR] backup: acquiring lock: ", err)
		return
	}
	if !got {
		return
	}

	log.Printf("Backup %s started on node %s/%s", bcp.Name, nodeInfo.SetName, nodeInfo.Me)
	err = backup.Run(bcp, a.pbm, a.node)
	if err != nil {
		log.Println("[ERROR] backup:", err)
	}
	log.Printf("Backup %s finished", bcp.Name)

	// wait before release lock in case backup was super fast and
	// other agents still trying to get lock
	time.Sleep(1000 * time.Millisecond)
	err = a.pbm.ReleaseLock(lock)
	if err != nil {
		log.Printf("[ERROR] backup: unable to release backup lock for %v:%v\n", lock, err)
	}
}
