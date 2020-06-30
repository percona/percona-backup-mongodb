package pitr

import (
	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Recovery struct {
	cn   *pbm.PBM
	node *pbm.Node
}

// NewRecovery creates a new restore object
func NewRecovery(cn *pbm.PBM, node *pbm.Node) *Recovery {
	return &Recovery{
		cn:   cn,
		node: node,
	}
}

// Run runs the recovery process
func (r *Recovery) Run(cmd pbm.PITRestoreCmd) (err error) {
	im, err := r.node.GetIsMaster()
	if err != nil {
		return errors.Wrap(err, "get isMaster data")
	}

	ts := primitive.Timestamp{T: uint32(cmd.TS), I: 0}

	lastChunk, err := r.cn.PITRGetChunk(im.SetName, ts)
	if err != nil {
		return errors.Wrap(err, "define last oplog slice")
	}

	snapshot, err := r.cn.GetLastBackup(&lastChunk.EndTS)

	err := r.verifyChunks(snapshot.LastWriteTS, lastChunk.StartTS)
	if err != nil {
		return errors.Wrap(err, "verify oplog slices chain")
	}

	return nil
}

func (r *Recovery) verifyChunks(from, to primitive.Timestamp) error {

}
