package pbm

import (
	"strings"

	"go.mongodb.org/mongo-driver/bson/primitive"

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
	// !!! ----
	// TODO: move somevere else. All finds won't work becase hb always changes
	Heartbeat primitive.Timestamp `bson:"hb"`
}

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

	l.Heartbeat, err = p.readClusterTime()
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	_, err = c.InsertOne(p.ctx, l)
	if err != nil && !strings.Contains(err.Error(), "E11000 duplicate key error") {
		return false, errors.Wrap(err, "aquire lock")
	}

	// if there is no error so we got index
	return err == nil, nil
}

func (p *PBM) LockBeat(l Lock) error {
	ts, err := p.readClusterTime()
	if err != nil {
		return errors.Wrap(err, "read cluster time")
	}

	_, err = p.Conn.Database(DB).Collection(OpCollection).UpdateOne(
		p.ctx,
		l,
		bson.M{"$set": bson.M{"hb": ts}},
	)
	return errors.Wrap(err, "set timestamp")
}

const staleFrameSec uint32 = 60

// LockIsStale checks whether existed lock with from the same Replset is stale
func (p *PBM) LockIsStale(l Lock) (bool, Lock, error) {
	var lock Lock

	ts, err := p.readClusterTime()
	if err != nil {
		return false, lock, errors.Wrap(err, "read cluster time")
	}

	err = p.Conn.Database(DB).Collection(OpCollection).
		FindOne(p.ctx, bson.M{"replset": l.Replset}).Decode(&lock)
	if err != nil {
		return false, lock, errors.Wrap(err, "get lock")
	}

	return lock.Heartbeat.T+staleFrameSec < ts.T, lock, nil
}

func (p *PBM) readClusterTime() (primitive.Timestamp, error) {
	// Make a read to force the cluster timestamp update.
	// Otherwise, cluster timestamp could remain the same between `isMaster` reads, while in fact time has been moved forward.
	err := p.Conn.Database(DB).Collection(OpCollection).FindOne(p.ctx, bson.D{}).Err()
	if err != nil && err != mongo.ErrNoDocuments {
		return primitive.Timestamp{}, errors.Wrap(err, "void read")
	}

	im, err := p.GetIsMaster()
	if err != nil {
		return primitive.Timestamp{}, errors.Wrap(err, "get isMaster")
	}

	if im.ClusterTime == nil {
		return primitive.Timestamp{}, errors.Wrap(err, "no clusterTime in response")
	}

	return im.ClusterTime.ClusterTime, nil
}

func (p *PBM) ReleaseLock(l Lock) error {
	_, err := p.Conn.Database(DB).Collection(OpCollection).DeleteOne(p.ctx, l)
	return errors.Wrap(err, "deleteOne")
}

func (p *PBM) Cleanup(bcpName string) error {
	_, err := p.Conn.Database(DB).Collection(OpCollection).DeleteMany(p.ctx, bson.M{"backup": bcpName})
	return errors.Wrap(err, "deleteMany")
}
