package pbm

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

// LockHeader describes the lock. This data will be serialased into the mongo document.
type LockHeader struct {
	Type       Command `bson:"type"`
	Replset    string  `bson:"replset"`
	Node       string  `bson:"node"`
	BackupName string  `bson:"backup"`
}

type lock struct {
	LockHeader `bson:",inline"`
	Heartbeat  primitive.Timestamp `bson:"hb"` // separated in order the lock can be searchable by the header
}

// Lock is a lock for the PBM operation (e.g. backup, restore)
type Lock struct {
	// mlock lock
	lock
	p      *PBM
	c      *mongo.Collection
	cancel context.CancelFunc
}

// NewLock creates a new Lock object from geven header. Returned lock has no state.
// So Acquire() and Release() methods should be called.
func (p *PBM) NewLock(h LockHeader) *Lock {
	return &Lock{
		lock: lock{
			LockHeader: h,
		},
		p: p,
		c: p.Conn.Database(DB).Collection(OpCollection),
	}
}

type ErrConcurrentOp struct {
	Lock LockHeader
}

func (e ErrConcurrentOp) Error() string {
	return fmt.Sprintf("another operation is running: %s '%s'", e.Lock.Type, e.Lock.BackupName)
}

// Acquire tries to acquire the lock.
// It returns true in case of success and false if there is
// lock already acquired by another process or some error happend.
// In case there is already concurrent lock exists, it checks if the concurrent lock isn't stale
// and clear the rot and tries again if it's happened to be so.
func (l *Lock) Acquire() (bool, error) {
	got, err := l.acquire()

	if err != nil {
		return false, err
	}

	if got {
		return true, nil
	}

	// there is some concurrent lock
	peer, err := l.getPeer()
	if err != nil {
		return false, errors.Wrap(err, "check for the peer")
	}

	ts, err := l.p.ClusterTime()
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	// peer is alive
	if peer.Heartbeat.T+staleFrameSec >= ts.T {
		if l.BackupName != peer.BackupName {
			return false, ErrConcurrentOp{Lock: peer.LockHeader}
		}
		return false, nil
	}

	_, err = l.c.DeleteOne(l.p.Context(), peer.LockHeader)
	if err != nil {
		return false, errors.Wrap(err, "delete stale lock")
	}

	return l.acquire()
}

// Release the lock
func (l *Lock) Release() error {
	if l.cancel != nil {
		l.cancel()
	}

	_, err := l.c.DeleteOne(l.p.Context(), l.LockHeader)
	return errors.Wrap(err, "deleteOne")
}

const staleFrameSec uint32 = 30

//
func (l *Lock) getPeer() (lock, error) {
	var peer lock

	err := l.c.FindOne(l.p.Context(), bson.M{"replset": l.Replset}).Decode(&peer)
	if err != nil {
		return peer, errors.Wrap(err, "get lock")
	}

	return peer, nil
}

func (l *Lock) acquire() (bool, error) {
	var err error
	l.Heartbeat, err = l.p.ClusterTime()
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	_, err = l.c.InsertOne(l.p.Context(), l.lock)
	if err != nil && !strings.Contains(err.Error(), "E11000 duplicate key error") {
		return false, errors.Wrap(err, "aquire lock")
	}

	// if there is no duplicate key error, we got the lock
	if err == nil {
		l.hb()
		return true, nil
	}

	return false, nil
}

// heartbeats for the lock
func (l *Lock) hb() {
	var ctx context.Context
	ctx, l.cancel = context.WithCancel(context.Background())
	go func() {
		tk := time.NewTicker(time.Second * 5)
		defer tk.Stop()
		for {
			select {
			case <-tk.C:
				err := l.beat()
				if err != nil {
					log.Println("[ERROR] lock heartbeat:", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (l *Lock) beat() error {
	ts, err := l.p.ClusterTime()
	if err != nil {
		return errors.Wrap(err, "read cluster time")
	}

	_, err = l.c.UpdateOne(
		l.p.Context(),
		l.LockHeader,
		bson.M{"$set": bson.M{"hb": ts}},
	)
	return errors.Wrap(err, "set timestamp")
}
