package pbm

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type StatusPITR struct {
	ID    string `bson:"_id"`
	Epoch int64  `bson:"epoch"`
}

const epochDocID = "pitrepoch"

func (p *PBM) SetEpoch(e int64) error {
	_, err := p.Conn.Database(DB).Collection(StatusCollection).UpdateOne(
		p.ctx,
		bson.D{{"_id", epochDocID}},
		bson.D{{"epoch", e}},
		options.Update().SetUpsert(true),
	)

	return err
}
func (p *PBM) GetEpoch() (int64, error) {
	e := &StatusPITR{}
	err := p.Conn.Database(DB).Collection(StatusCollection).FindOne(
		p.ctx,
		bson.D{{"_id", epochDocID}},
	).Decode(e)

	if err != nil {
		return 0, err
	}

	return e.Epoch, nil
}
