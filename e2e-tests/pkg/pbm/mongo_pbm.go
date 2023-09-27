package pbm

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/lock"
	"github.com/percona/percona-backup-mongodb/internal/query"
	"github.com/percona/percona-backup-mongodb/internal/resync"
	"github.com/percona/percona-backup-mongodb/internal/storage"
	"github.com/percona/percona-backup-mongodb/internal/topo"
	"github.com/percona/percona-backup-mongodb/internal/types"
	"github.com/percona/percona-backup-mongodb/internal/util"
	"github.com/percona/percona-backup-mongodb/pbm"
)

type MongoPBM struct {
	p   *pbm.PBM
	ctx context.Context
}

func NewMongoPBM(ctx context.Context, connectionURI string) (*MongoPBM, error) {
	cn, err := pbm.New(ctx, connectionURI, "e2e-tests-pbm")
	if err != nil {
		return nil, errors.Wrap(err, "connect")
	}

	cn.InitLogger("", "")

	return &MongoPBM{
		p:   cn,
		ctx: ctx,
	}, nil
}

func (m *MongoPBM) SendCmd(cmd types.Cmd) error {
	cmd.TS = time.Now().UTC().Unix()
	_, err := m.p.Conn.CmdStreamCollection().InsertOne(m.ctx, cmd)
	return err
}

func (m *MongoPBM) BackupsList(ctx context.Context, limit int64) ([]types.BackupMeta, error) {
	return query.BackupsList(ctx, m.p.Conn, limit)
}

func (m *MongoPBM) GetBackupMeta(ctx context.Context, bcpName string) (*types.BackupMeta, error) {
	return query.GetBackupMeta(ctx, m.p.Conn, bcpName)
}

func (m *MongoPBM) DeleteBackup(ctx context.Context, bcpName string) error {
	l := m.p.Logger().NewEvent(string(defs.CmdDeleteBackup), "", "", primitive.Timestamp{})
	return m.p.DeleteBackup(ctx, bcpName, l)
}

func (m *MongoPBM) Storage(ctx context.Context) (storage.Storage, error) {
	l := m.p.Logger().NewEvent("", "", "", primitive.Timestamp{})
	return util.GetStorage(ctx, m.p.Conn, l)
}

func (m *MongoPBM) StoreResync(ctx context.Context) error {
	l := m.p.Logger().NewEvent(string(defs.CmdResync), "", "", primitive.Timestamp{})
	return resync.ResyncStorage(ctx, m.p.Conn, l)
}

func (m *MongoPBM) Conn() connect.Client {
	return m.p.Conn
}

// WaitOp waits up to waitFor duration until operations which acquires a given lock are finished
func (m *MongoPBM) WaitOp(ctx context.Context, lck *lock.LockHeader, waitFor time.Duration) error {
	return m.waitOp(ctx, lck, waitFor, lock.GetLockData)
}

// WaitConcurentOp waits up to waitFor duration until operations which acquires a given lock are finished
func (m *MongoPBM) WaitConcurentOp(ctx context.Context, lck *lock.LockHeader, waitFor time.Duration) error {
	return m.waitOp(ctx, lck, waitFor, lock.GetOpLockData)
}

// WaitOp waits up to waitFor duration until operations which acquires a given lock are finished
func (m *MongoPBM) waitOp(
	ctx context.Context,
	lck *lock.LockHeader,
	waitFor time.Duration,
	f func(ctx context.Context, m connect.Client, lh *lock.LockHeader) (lock.LockData, error),
) error {
	// just to be sure the check hasn't started before the lock were created
	time.Sleep(1 * time.Second)

	tmr := time.NewTimer(waitFor)
	tkr := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-tmr.C:
			return errors.Errorf("timeout reached")
		case <-tkr.C:
			lock, err := f(ctx, m.p.Conn, lck)
			if err != nil {
				// No lock, so operation has finished
				if errors.Is(err, mongo.ErrNoDocuments) {
					return nil
				}
				return errors.Wrap(err, "get lock data")
			}
			clusterTime, err := topo.GetClusterTime(ctx, m.p.Conn)
			if err != nil {
				return errors.Wrap(err, "read cluster time")
			}
			if lock.Heartbeat.T+defs.StaleFrameSec < clusterTime.T {
				return errors.Errorf("operation stale, last beat ts: %d", lock.Heartbeat.T)
			}
		}
	}
}
