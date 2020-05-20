package pbm

import (
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	// PITRdiscrepancy oplog sliceing discrepancy
	PITRdiscrepancy = time.Minute * 5
	// PITRfsPrefix is a prefix (folder) for PITR chunks on the storage
	PITRfsPrefix = "pbmPITR"
)

// PITRLock is a lock should be aquired by the node in order
// to maintain incremental backups (oplog sliceing) for the replicaset
type PITRLock struct {
	PITRLockHeader `bson:",inline"`
	LastTS         primitive.Timestamp `bson:"last_ts"`
	Heartbeat      primitive.Timestamp `bson:"hb"`
}

// PITRLockHeader is the header of PITR lock
// separated in order to make it easier to search by header fields
type PITRLockHeader struct {
	Replset string `bson:"replset,omitempty"`
	Node    string `bson:"node,omitempty"`
}

// PITRIndex is an index for the faster search of chunks needed for PITR
type PITRIndex struct {
	Replsets []struct {
		Name   string              `bson:"name"`
		LastTS primitive.Timestamp `bson:"last_ts"`
		Chunks []PITRChunk         `bson:"chunks"`
	} `bson:"replsets"`
}

// PITRChunk is index metadata for the oplog chunk
type PITRChunk struct {
	FName   string              `bson:"file_name"`
	StartTS primitive.Timestamp `bson:"start_ts"`
	LastTS  primitive.Timestamp `bson:"last_ts"`
}

func (p *PBM) IsPITR() (bool, error) {
	cfg, err := p.GetConfig()
	if err != nil {
		return false, errors.Wrap(err, "get config")
	}

	return cfg.PITR.Enabled, nil
}
