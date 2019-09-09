package pbm

import (
	"strings"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Lock struct {
	Type       Command `bson:"type"`
	Replset    string  `bson:"replset"`
	Node       string  `bson:"node"`
	BackupName string  `bson:"backup"`
}

var errLocked = errors.New("locked")

// AcquireLock tries to acquire lock on operation (e.g. backup, restore).
// It returns true in case of success and false if there is
// lock already acquired by another process or some error happend.
func (p *PBM) AcquireLock(l Lock) (bool, error) {
	c := p.Conn.Database(DB).Collection(OpCollection)
	_, err := c.Indexes().CreateOne(
		p.ctx,
		mongo.IndexModel{
			Keys: bson.D{{"replset", 1}},
			Options: options.Index().
				SetUnique(true).
				SetSparse(true),
		},
	)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return false, errors.Wrap(err, "ensure lock index")
	}

	_, err = c.InsertOne(p.ctx, l)
	if err != nil && !strings.Contains(err.Error(), "E11000 duplicate key error") {
		return false, errors.Wrap(err, "aquire lock")
	}

	// if there is no error so we got index
	return err == nil, nil
}

func (p *PBM) ReleaseLock(l Lock) error {
	_, err := p.Conn.Database(DB).Collection(OpCollection).DeleteOne(nil, l)
	return errors.Wrap(err, "deleteOne")
}
