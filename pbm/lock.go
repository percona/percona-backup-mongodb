package pbm

import (
	"context"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
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
	sess, err := p.Conn.StartSession(
		options.Session().
			SetDefaultReadPreference(readpref.Primary()).
			SetCausalConsistency(true).
			SetDefaultReadConcern(readconcern.Majority()).
			SetDefaultWriteConcern(writeconcern.New(writeconcern.WMajority())),
	)
	if err != nil {
		return false, errors.Wrap(err, "start session")
	}
	defer sess.EndSession(context.Background())

	err = sess.StartTransaction()
	if err != nil {
		return false, errors.Wrap(err, "start transaction")
	}

	err = mongo.WithSession(context.Background(), sess, func(sc mongo.SessionContext) error {
		var err error
		defer func() {
			if err != nil {
				sess.AbortTransaction(sc)
			}
		}()

		lc := Lock{}
		err = p.opC.FindOne(sc, bson.D{{"replset", l.Replset}}).Decode(&lc)
		if err != nil && err != mongo.ErrNoDocuments {
			return errors.Wrap(err, "find lock")
		} else if err == nil {
			return errLocked
		}

		_, err = p.opC.InsertOne(sc, l)
		if err != nil {
			return errors.Wrap(err, "insert lock")
		}

		return sess.CommitTransaction(sc)
	})

	if err != nil {
		if err == errLocked {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (p *PBM) ReleaseLock(l Lock) error {
	_, err := p.opC.DeleteOne(nil, l)
	return err
}
