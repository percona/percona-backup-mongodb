package pbm

import (
	"fmt"
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
	return p.pitrChunk(rs, -1)
}

// PITRFirstChunkMeta returns the oldest PITR chunk for the given Replset
func (p *PBM) PITRFirstChunkMeta(rs string) (*PITRChunk, error) {
	return p.pitrChunk(rs, 1)
}

func (p *PBM) pitrChunk(rs string, sort int) (*PITRChunk, error) {
	res := p.Conn.Database(DB).Collection(PITRChunksCollection).FindOne(
		p.ctx,
		bson.D{{"rs", rs}},
		options.FindOne().SetSort(bson.D{{"start_ts", sort}}),
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

// PITRGetChunkContains returns a pitr slice chunk that belongs to the
// given replica set and contains the given timestamp
func (p *PBM) PITRGetChunkContains(rs string, ts primitive.Timestamp) (*PITRChunk, error) {
	res := p.Conn.Database(DB).Collection(PITRChunksCollection).FindOne(
		p.ctx,
		bson.D{
			{"rs", rs},
			{"start_ts", bson.M{"$lte": ts}},
			{"end_ts", bson.M{"$gte": ts}},
		},
	)
	if res.Err() != nil {
		return nil, errors.Wrap(res.Err(), "get")
	}

	chnk := new(PITRChunk)
	err := res.Decode(chnk)
	return chnk, errors.Wrap(err, "decode")
}

// PITRGetChunksSlice returns slice of PITR oplog chunks which Start TS
// lies in a given time frame
func (p *PBM) PITRGetChunksSlice(rs string, from, to primitive.Timestamp) ([]PITRChunk, error) {
	cur, err := p.Conn.Database(DB).Collection(PITRChunksCollection).Find(
		p.ctx,
		bson.D{
			{"rs", rs},
			{"start_ts", bson.M{"$gte": from, "$lte": to}},
		},
		options.Find().SetSort(bson.D{{"start_ts", 1}}),
	)

	if err != nil {
		return nil, errors.Wrap(err, "get cursor")
	}
	defer cur.Close(p.ctx)

	chnks := []PITRChunk{}

	for cur.Next(p.ctx) {
		var chnk PITRChunk
		err := cur.Decode(&chnk)
		if err != nil {
			return nil, errors.Wrap(err, "decode chunk")
		}

		chnks = append(chnks, chnk)
	}

	return chnks, cur.Err()
}

// PITRGetChunkStarts returns a pitr slice chunk that belongs to the
// given replica set and start from the given timestamp
func (p *PBM) PITRGetChunkStarts(rs string, ts primitive.Timestamp) (*PITRChunk, error) {
	res := p.Conn.Database(DB).Collection(PITRChunksCollection).FindOne(
		p.ctx,
		bson.D{
			{"rs", rs},
			{"start_ts", ts},
		},
	)
	if res.Err() != nil {
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

type Timeline struct {
	Start uint32
	End   uint32
}

func (t *Timeline) String() string {
	ts := time.Unix(int64(t.Start), 0).UTC()
	te := time.Unix(int64(t.End), 0).UTC()
	return fmt.Sprintf("%s - %s", ts.Format(time.RFC3339), te.Format(time.RFC3339))
}

// PITRGetValidTimelines returns time ranges valid for PITR restore
// for the given replicaset. We don't check for any "restore intrusions"
// or other integrity issues since it's guaranteed be the slicer that
// any saved chunk already belongs to some valid timeline,
// the slice wouldn't be done otherwise
func (p *PBM) PITRGetValidTimelines(rs string, until int64) (tlines []Timeline, err error) {
	fch, err := p.PITRFirstChunkMeta(rs)
	if err != nil {
		return nil, errors.Wrap(err, "get the oldest chunk")
	}
	if fch == nil {
		return nil, nil
	}

	slices, err := p.PITRGetChunksSlice(rs, fch.StartTS, primitive.Timestamp{T: uint32(until), I: 0})
	if err != nil {
		return nil, errors.Wrap(err, "get slice")
	}

	return gettimelines(slices), nil
}

func gettimelines(slices []PITRChunk) (tlines []Timeline) {
	var tl Timeline
	var prevEnd uint32
	for _, s := range slices {
		if prevEnd != 0 && prevEnd != s.StartTS.T {
			tlines = append(tlines, tl)
			tl = Timeline{}
		}
		if tl.Start == 0 {
			tl.Start = s.StartTS.T
		}
		prevEnd = s.EndTS.T
		tl.End = s.EndTS.T
	}

	tlines = append(tlines, tl)

	return tlines
}
