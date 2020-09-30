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

const StaleFrameSec uint32 = 30

// LockHeader describes the lock. This data will be serialased into the mongo document.
type LockHeader struct {
	Type       Command `bson:"type,omitempty"`
	Replset    string  `bson:"replset,omitempty"`
	Node       string  `bson:"node,omitempty"`
	BackupName string  `bson:"backup,omitempty"`
}

type LockData struct {
	LockHeader `bson:",inline"`
	Heartbeat  primitive.Timestamp `bson:"hb"` // separated in order the lock can be searchable by the header
}

// Lock is a lock for the PBM operation (e.g. backup, restore)
type Lock struct {
	LockData
	p        *PBM
	c        *mongo.Collection
	cancel   context.CancelFunc
	hbRate   time.Duration
	staleSec uint32
}

// NewLock creates a new Lock object from geven header. Returned lock has no state.
// So Acquire() and Release() methods should be called.
func (p *PBM) NewLock(h LockHeader) *Lock {
	return p.newLock(h, LockCollection)
}

// NewLockCol creates a new Lock object from geven header in given collection.
// Returned lock has no state. So Acquire() and Release() methods should be called.
func (p *PBM) NewLockCol(h LockHeader, collection string) *Lock {
	return p.newLock(h, collection)
}

func (p *PBM) newLock(h LockHeader, col string) *Lock {
	return &Lock{
		LockData: LockData{
			LockHeader: h,
		},
		p:        p,
		c:        p.Conn.Database(DB).Collection(col),
		hbRate:   time.Second * 5,
		staleSec: StaleFrameSec,
	}
}

// ErrConcurrentOp means lock was already aquired by another node
type ErrConcurrentOp struct {
	Lock LockHeader
}

func (e ErrConcurrentOp) Error() string {
	return fmt.Sprintf("another operation is running: %s '%s'", e.Lock.Type, e.Lock.BackupName)
}

// ErrWasStaleLock - the lock was already got but the operation seems to be staled (no hb from the node)
type ErrWasStaleLock struct {
	Lock LockHeader
}

func (e ErrWasStaleLock) Error() string {
	return fmt.Sprintf("was stale lock: %s '%s'", e.Lock.Type, e.Lock.BackupName)
}

// Acquire tries to acquire the lock.
// It returns true in case of success and false if
// a lock already acquired by another process or some error happened.
// In case of concurrent lock exists is stale it will be deleted and
// ErrWasStaleLock gonna be returned. A client shell mark respective operation
// as stale and retry if it needs to
func (l *Lock) Acquire() (bool, error) {
	got, err := l.acquire()

	if err != nil {
		return false, err
	}

	if got {
		return true, nil
	}

	// there is some concurrent lock
	peer, err := l.p.getLockData(&LockHeader{Replset: l.Replset}, l.c)
	if err != nil {
		return false, errors.Wrap(err, "check for the peer")
	}

	ts, err := l.p.ClusterTime()
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	// peer is alive
	if peer.Heartbeat.T+l.staleSec >= ts.T {
		if l.BackupName != peer.BackupName {
			return false, ErrConcurrentOp{Lock: peer.LockHeader}
		}
		return false, nil
	}

	_, err = l.c.DeleteOne(l.p.Context(), peer.LockHeader)
	if err != nil {
		return false, errors.Wrap(err, "delete stale lock")
	}

	return false, ErrWasStaleLock{Lock: peer.LockHeader}
}

func (p *PBM) MarkBcpStale(bcpName string) error {
	bcp, err := p.GetBackupMeta(bcpName)
	if err != nil {
		return errors.Wrap(err, "get backup meta")
	}

	// not to rewrite an error emitted by the agent
	if bcp.Status == StatusError {
		return nil
	}

	return p.ChangeBackupState(bcpName, StatusError, "some of pbm-agents were lost during the backup")
}

func (p *PBM) MarkRestoreStale(name string) error {
	r, err := p.GetRestoreMeta(name)
	if err != nil {
		return errors.Wrap(err, "get retore meta")
	}

	// not to rewrite an error emitted by the agent
	if r.Status == StatusError {
		return nil
	}

	return p.ChangeRestoreState(name, StatusError, "some of pbm-agents were lost during the restore")
}

// Release the lock
func (l *Lock) Release() error {
	if l.cancel != nil {
		l.cancel()
	}

	_, err := l.c.DeleteOne(l.p.Context(), l.LockHeader)
	return errors.Wrap(err, "deleteOne")
}

func (l *Lock) acquire() (bool, error) {
	var err error
	l.Heartbeat, err = l.p.ClusterTime()
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	_, err = l.c.InsertOne(l.p.Context(), l.LockData)
	if err != nil && !strings.Contains(err.Error(), "E11000 duplicate key error") {
		return false, errors.Wrap(err, "acquire lock")
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
		tk := time.NewTicker(l.hbRate)
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

func (p *PBM) GetLockData(lh *LockHeader) (LockData, error) {
	return p.getLockData(lh, p.Conn.Database(DB).Collection(LockCollection))
}

func (p *PBM) getLockData(lh *LockHeader, cl *mongo.Collection) (LockData, error) {
	var l LockData
	r := cl.FindOne(p.ctx, lh)
	if r.Err() != nil {
		return l, r.Err()
	}
	err := r.Decode(&l)
	return l, err
}

func (p *PBM) GetLocks(lh *LockHeader) ([]LockData, error) {
	return p.getLocks(lh, p.Conn.Database(DB).Collection(LockCollection))
}

func (p *PBM) getLocks(lh *LockHeader, cl *mongo.Collection) ([]LockData, error) {
	var locks []LockData

	cur, err := cl.Find(p.ctx, lh)
	if err != nil {
		return nil, errors.Wrap(err, "get locks")
	}

	for cur.Next(p.ctx) {
		var l LockData
		err := cur.Decode(&l)
		if err != nil {
			return nil, errors.Wrap(err, "lock decode")
		}

		locks = append(locks, l)
	}

	return locks, cur.Err()
}
