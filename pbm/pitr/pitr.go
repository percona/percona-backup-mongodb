package pitr

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/pkg/errors"
)

type IBackup struct {
	pbm    *pbm.PBM
	node   *pbm.Node
	ctx    context.Context
	lastTS primitive.Timestamp
	oplog  *backup.Oplog
	span   time.Duration
}

// Catchup seeks for the last saved (backuped) TS - the starting point.  It should be run only
// if the timeline was lost (e.g. on (re)start or another node's fail).
// The starting point sets to the last backup's or last PITR chunk's TS whichever is more recent
func (i *IBackup) Catchup() error {
	bcp, err := i.pbm.GetLastBackup()
	if err != nil {
		return errors.Wrap(err, "get last backup")
	}
	if bcp == nil {
		return errors.New("no backup found")
	}

	i.lastTS = bcp.LastWriteTS

	chnk, err := i.pbm.PITRLastChunkMeta()
	if err != nil {
		return errors.Wrap(err, "get last backup")
	}

	if chnk == nil {
		return nil
	}

	if chnk.EndTS.T > i.lastTS.T {
		i.lastTS = chnk.EndTS
	}

	return nil
}

func (i *IBackup) Do() error {
	if i.lastTS.T == 0 {
		return errors.New("no starting point defined")
	}
	tk := time.NewTicker(i.span)
	defer tk.Stop()

	oplog := backup.NewOplog(i.node)
	for range tk.C {
		now, err := oplog.LastWrite()
		if err != nil {
			return errors.Wrap(err, "define last write timestamp")
		}
		oplog.SetTailingSpan(i.lastTS, now)
	}

	return nil
}
