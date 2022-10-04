package pbm

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/storage"
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

func (m *MongoPBM) SendCmd(cmd pbm.Cmd) error {
	return m.p.SendCmd(cmd)
}

func (m *MongoPBM) BackupsList(limit int64) ([]pbm.BackupMeta, error) {
	return m.p.BackupsList(limit)
}

func (m *MongoPBM) GetBackupMeta(bcpName string) (*pbm.BackupMeta, error) {
	return m.p.GetBackupMeta(bcpName)
}

func (m *MongoPBM) DeleteBackup(bcpName string) error {
	return m.p.DeleteBackup(bcpName, true, m.p.Logger().NewEvent(string(pbm.CmdDeleteBackup), "", "", primitive.Timestamp{}))
}

func (m *MongoPBM) Storage() (storage.Storage, error) {
	return m.p.GetStorage(m.p.Logger().NewEvent("", "", "", primitive.Timestamp{}))
}

func (m *MongoPBM) StoreResync() error {
	return m.p.ResyncStorage(m.p.Logger().NewEvent(string(pbm.CmdResync), "", "", primitive.Timestamp{}))
}

func (m *MongoPBM) Conn() *mongo.Client {
	return m.p.Conn
}

// WaitOp waits up to waitFor duration until operations which acquires a given lock are finished
func (m *MongoPBM) WaitOp(lock *pbm.LockHeader, waitFor time.Duration) error {
	return m.waitOp(lock, waitFor, m.p.GetLockData)
}

// WaitConcurentOp waits up to waitFor duration until operations which acquires a given lock are finished
func (m *MongoPBM) WaitConcurentOp(lock *pbm.LockHeader, waitFor time.Duration) error {
	return m.waitOp(lock, waitFor, m.p.GetOpLockData)
}

// WaitOp waits up to waitFor duration until operations which acquires a given lock are finished
func (m *MongoPBM) waitOp(lock *pbm.LockHeader, waitFor time.Duration, f func(*pbm.LockHeader) (pbm.LockData, error)) error {
	// just to be sure the check hasn't started before the lock were created
	time.Sleep(1 * time.Second)

	tmr := time.NewTimer(waitFor)
	tkr := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-tmr.C:
			return errors.Errorf("timeout reached")
		case <-tkr.C:
			lock, err := f(lock)
			if err != nil {
				// No lock, so operation has finished
				if err == mongo.ErrNoDocuments {
					return nil
				}
				return errors.Wrap(err, "get lock data")
			}
			clusterTime, err := m.p.ClusterTime()
			if err != nil {
				return errors.Wrap(err, "read cluster time")
			}
			if lock.Heartbeat.T+pbm.StaleFrameSec < clusterTime.T {
				return errors.Errorf("operation stale, last beat ts: %d", lock.Heartbeat.T)
			}
		}
	}
}
