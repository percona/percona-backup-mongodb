package pbm

import (
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// PITRdiscrepancy oplog sliceing discrepancy
	PITRdiscrepancy = time.Minute * 5
	// PITRfsPrefix is a prefix (folder) for PITR chunks on the storage
	PITRfsPrefix = "pbmPITR"
)

// PITRMeta is an index for the faster search of chunks needed for PITR
type PITRMeta struct {
	Status   Status        `bson:"status"`
	Replsets []PITRReplset `bson:"replsets"`
}

type PITRReplset struct {
	Name    string              `bson:"name"`
	FirstTS primitive.Timestamp `bson:"first_ts"`
	LastTS  primitive.Timestamp `bson:"last_ts"`
	Chunks  []PITRChunk         `bson:"chunks"`
}

// PITRChunk is index metadata for the oplog chunk
type PITRChunk struct {
	FName       string              `bson:"file_name"`
	Compression CompressionType     `bson:"compression"`
	StartTS     primitive.Timestamp `bson:"start_ts"`
	EndTS       primitive.Timestamp `bson:"end_ts"`
}

func (p *PBM) IsPITR() (bool, error) {
	cfg, err := p.GetConfig()
	if err != nil {
		return false, errors.Wrap(err, "get config")
	}

	return cfg.PITR.Enabled, nil
}

func (p *PBM) PITRLastChunkMeta() (*PITRChunk, error) {
	res := p.Conn.Database(DB).Collection(PITRCollection).FindOne(
		p.ctx,
		bson.D{{"status", StatusDone}},
		options.FindOne().SetSort(bson.D{{"end_ts", -1}}),
	)
	if res.Err() != nil {
		if res.Err() == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, errors.Wrap(res.Err(), "get")
	}

	chnk := new(PITRChunk)
	err := res.Decode(chnk)
	return chnk, errors.Wrap(err, "decode")
}
