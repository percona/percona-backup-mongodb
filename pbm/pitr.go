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

// PITRChunk is index metadata for the oplog chunks
type PITRChunk struct {
	RS          string              `bson:"rs"`
	FName       string              `bson:"fname"`
	Compression CompressionType     `bson:"compression"`
	StartTS     primitive.Timestamp `bson:"start_ts"`
	EndTS       primitive.Timestamp `bson:"end_ts"`
}

// IsPITR checks if PITR is enabled
func (p *PBM) IsPITR() (bool, error) {
	cfg, err := p.GetConfig()
	if err != nil {
		return false, errors.Wrap(err, "get config")
	}

	return cfg.PITR.Enabled, nil
}

// PITRLastChunkMeta returns the most recent PITR chunk for the given Replset
func (p *PBM) PITRLastChunkMeta(rs string) (*PITRChunk, error) {
	res := p.Conn.Database(DB).Collection(PITRChunksCollection).FindOne(
		p.ctx,
		bson.D{{"rs", rs}},
		options.FindOne().SetSort(bson.D{{"start_ts", -1}}),
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

// PITRAddChunk stores PITR chunk metadata
func (p *PBM) PITRAddChunk(c PITRChunk) error {
	_, err := p.Conn.Database(DB).Collection(PITRChunksCollection).InsertOne(p.ctx, c)

	return err
}
