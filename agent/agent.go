package agent

import (
	"log"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/backup"
	"github.com/percona/percona-backup-mongodb/pbm"
)

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

func (a *Agent) ListenCmd() error {
	c, cerr, err := a.pbm.ListenCmd()
	if err != nil {
		return errors.Wrap(err, "listen commands stream")
	}

	for {
		select {
		case cmd := <-c:
			switch cmd.Cmd {
			case pbm.CmdBackup:
				log.Println("Backup started:", cmd.Backup.Name)
				err := backup.Backup(cmd.Backup, a.pbm, a.node)
				if err != nil {
					log.Println("[ERROR] backup:", err)
				}
				log.Println("Backup finished:", cmd.Backup.Name)
			case pbm.CmdRestore:
			}
		case err := <-cerr:
			log.Println(err)
		}
	}
}
