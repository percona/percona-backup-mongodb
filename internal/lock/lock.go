package lock

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/query"
	"github.com/percona/percona-backup-mongodb/internal/topo"
)

// LockHeader describes the lock. This data will be serialased into the mongo document.
type LockHeader struct {
	Type    defs.Command `bson:"type,omitempty" json:"type,omitempty"`
	Replset string       `bson:"replset,omitempty" json:"replset,omitempty"`
	Node    string       `bson:"node,omitempty" json:"node,omitempty"`
	OPID    string       `bson:"opid,omitempty" json:"opid,omitempty"`
	// should be a pointer so mongo find with empty epoch would work
	// otherwise it always set it at least to "epoch":{"$timestamp":{"t":0,"i":0}}
	Epoch *primitive.Timestamp `bson:"epoch,omitempty" json:"epoch,omitempty"`
}

type LockData struct {
	LockHeader `bson:",inline"`
	Heartbeat  primitive.Timestamp `bson:"hb"` // separated in order the lock can be searchable by the header
}

// Lock is a lock for the PBM operation (e.g. backup, restore)
type Lock struct {
	LockData
	m        connect.Client
	coll     *mongo.Collection
	cancel   context.CancelFunc
	hbRate   time.Duration
	staleSec uint32
}

// NewLock creates a new Lock object from geven header. Returned lock has no state.
// So Acquire() and Release() methods should be called.
func NewLock(m connect.Client, h LockHeader) *Lock {
	return newLock(m, m.LockCollection(), h)
}

// NewOpLock creates a new Lock object from geven header in given op.
// Returned lock has no state. So Acquire() and Release() methods should be called.
func NewOpLock(m connect.Client, h LockHeader) *Lock {
	return newLock(m, m.LockOpCollection(), h)
}

func newLock(m connect.Client, coll *mongo.Collection, h LockHeader) *Lock {
	return &Lock{
		LockData: LockData{
			LockHeader: h,
		},
		m:        m,
		coll:     coll,
		hbRate:   time.Second * 5,
		staleSec: defs.StaleFrameSec,
	}
}

// Rewrite tries to acquire the lock instead the `old` one.
// It returns true in case of success and false if
// a lock already acquired by another process or some error happened.
// In case of concurrent lock exists is stale it will be deleted and
// ErrWasStaleLock gonna be returned. A client shell mark respective operation
// as stale and retry if it needs to
func (l *Lock) Rewrite(ctx context.Context, old *LockHeader) (bool, error) {
	return l.try(ctx, old)
}

// Acquire tries to acquire the lock.
// It returns true in case of success and false if
// a lock already acquired by another process or some error happened.
// In case of concurrent lock exists is stale it will be deleted and
// ErrWasStaleLock gonna be returned. A client shell mark respective operation
// as stale and retry if it needs to
func (l *Lock) Acquire(ctx context.Context) (bool, error) {
	return l.try(ctx, nil)
}

func (l *Lock) try(ctx context.Context, old *LockHeader) (bool, error) {
	var got bool
	var err error

	if old != nil {
		got, err = l.rewrite(ctx, old)
	} else {
		got, err = l.acquire(ctx)
	}

	if err != nil {
		return false, err
	}

	if got {
		// log the operation. duplicate means error
		err := l.log(ctx)
		if err != nil {
			rerr := l.Release()
			if rerr != nil {
				err = errors.Errorf("%v. Also failed to release the lock: %v", err, rerr)
			}
			return false, err
		}
		return true, nil
	}

	// there is some concurrent lock
	peer, err := getLockData(ctx, &LockHeader{Replset: l.Replset}, l.coll)
	if err != nil {
		return false, errors.Wrap(err, "check for the peer")
	}

	ts, err := topo.GetClusterTime(ctx, l.m)
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	// peer is alive
	if peer.Heartbeat.T+l.staleSec >= ts.T {
		if l.OPID != peer.OPID {
			return false, ConcurrentOpError{Lock: peer.LockHeader}
		}
		return false, nil
	}

	_, err = l.coll.DeleteOne(ctx, peer.LockHeader)
	if err != nil {
		return false, errors.Wrap(err, "delete stale lock")
	}

	return false, StaleLockError{Lock: peer.LockHeader}
}

func (l *Lock) log(ctx context.Context) error {
	// PITR slicing technically speaking is not an OP but
	// long standing process. It souldn't be logged. Moreover
	// having no opid it would block all subsequent PITR events.
	if l.LockHeader.Type == defs.CmdPITR {
		return nil
	}

	_, err := l.m.PBMOpLogCollection().InsertOne(ctx, l.LockHeader)
	if err != nil {
		if se, ok := err.(mongo.ServerError); ok && se.HasErrorCode(11000) { //nolint:errorlint
			return DuplicatedOpError{l.LockHeader}
		}
		return err
	}

	return nil
}

func MarkBcpStale(ctx context.Context, l *Lock, opid string) error {
	bcp, err := query.GetBackupByOPID(ctx, l.m, opid)
	if err != nil {
		return errors.Wrap(err, "get backup meta")
	}

	// not to rewrite an error emitted by the agent
	if bcp.Status == defs.StatusError || bcp.Status == defs.StatusDone {
		return nil
	}

	if logger := log.GetLoggerFromContextOr(ctx, nil); logger != nil {
		logger.Debug(string(defs.CmdBackup), "", opid, primitive.Timestamp{}, "mark stale meta")
	}
	return query.ChangeBackupStateOPID(l.m, opid, defs.StatusError,
		"some of pbm-agents were lost during the backup")
}

func MarkRestoreStale(ctx context.Context, l *Lock, opid string) error {
	r, err := query.GetRestoreMetaByOPID(ctx, l.m, opid)
	if err != nil {
		return errors.Wrap(err, "get retore meta")
	}

	// not to rewrite an error emitted by the agent
	if r.Status == defs.StatusError || r.Status == defs.StatusDone {
		return nil
	}

	if logger := log.GetLoggerFromContextOr(ctx, nil); logger != nil {
		logger.Debug(string(defs.CmdRestore), "", opid, primitive.Timestamp{}, "mark stale meta")
	}
	return query.ChangeRestoreStateOPID(ctx, l.m, opid, defs.StatusError,
		"some of pbm-agents were lost during the restore")
}

// Release the lock
func (l *Lock) Release() error {
	if l.cancel != nil {
		l.cancel()
	}

	_, err := l.coll.DeleteOne(context.Background(), l.LockHeader)
	return errors.Wrap(err, "deleteOne")
}

func (l *Lock) acquire(ctx context.Context) (bool, error) {
	var err error
	l.Heartbeat, err = topo.GetClusterTime(ctx, l.m)
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	_, err = l.coll.InsertOne(ctx, l.LockData)
	if err != nil {
		if se, ok := err.(mongo.ServerError); ok && se.HasErrorCode(11000) { //nolint:errorlint
			return false, nil
		}
		return false, errors.Wrap(err, "acquire lock")
	}

	l.hb(ctx)
	return true, nil
}

// rewrite tries to rewrite the given lock with itself
// it will transactionally delete the `old` lock
// and acquire an istance of itself
func (l *Lock) rewrite(ctx context.Context, old *LockHeader) (bool, error) {
	var err error
	l.Heartbeat, err = topo.GetClusterTime(ctx, l.m)
	if err != nil {
		return false, errors.Wrap(err, "read cluster time")
	}

	_, err = l.coll.DeleteOne(ctx, old)
	if err != nil {
		return false, errors.Wrap(err, "rewrite: delete old")
	}

	_, err = l.coll.InsertOne(ctx, l.LockData)

	if err != nil {
		if se, ok := err.(mongo.ServerError); ok && se.HasErrorCode(11000) { //nolint:errorlint
			return false, nil
		}
		return false, errors.Wrap(err, "acquire lock")
	}

	l.hb(ctx)
	return true, nil
}

// heartbeats for the lock
func (l *Lock) hb(ctx context.Context) {
	logger := log.GetLoggerFromContextOr(ctx, nil)
	ctx, l.cancel = context.WithCancel(ctx)

	go func() {
		tk := time.NewTicker(l.hbRate)
		defer tk.Stop()

		for {
			select {
			case <-tk.C:
				err := l.beat(ctx)
				if err != nil && logger != nil {
					logger.Error(string(l.Type), "", l.OPID, *l.Epoch, "send lock heartbeat: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (l *Lock) beat(ctx context.Context) error {
	ts, err := topo.GetClusterTime(ctx, l.m)
	if err != nil {
		return errors.Wrap(err, "read cluster time")
	}

	_, err = l.coll.UpdateOne(
		ctx,
		l.LockHeader,
		bson.M{"$set": bson.M{"hb": ts}},
	)
	return errors.Wrap(err, "set timestamp")
}

func GetLockData(ctx context.Context, m connect.Client, lh *LockHeader) (LockData, error) {
	return getLockData(ctx, lh, m.LockCollection())
}

func GetOpLockData(ctx context.Context, m connect.Client, lh *LockHeader) (LockData, error) {
	return getLockData(ctx, lh, m.LockOpCollection())
}

func getLockData(ctx context.Context, lh *LockHeader, cl *mongo.Collection) (LockData, error) {
	var l LockData
	r := cl.FindOne(ctx, lh)
	if r.Err() != nil {
		return l, r.Err()
	}
	err := r.Decode(&l)
	return l, err
}

func GetLocks(ctx context.Context, m connect.Client, lh *LockHeader) ([]LockData, error) {
	return getLocks(ctx, lh, m.LockCollection())
}

func GetOpLocks(ctx context.Context, m connect.Client, lh *LockHeader) ([]LockData, error) {
	return getLocks(ctx, lh, m.LockOpCollection())
}

func getLocks(ctx context.Context, lh *LockHeader, cl *mongo.Collection) ([]LockData, error) {
	var locks []LockData

	cur, err := cl.Find(ctx, lh)
	if err != nil {
		return nil, errors.Wrap(err, "get locks")
	}

	for cur.Next(ctx) {
		var l LockData
		err := cur.Decode(&l)
		if err != nil {
			return nil, errors.Wrap(err, "lock decode")
		}

		locks = append(locks, l)
	}

	return locks, cur.Err()
}
