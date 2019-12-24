package pkg

import (
	"context"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
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

	return &MongoPBM{
		p:   cn,
		ctx: ctx,
	}, nil
}

func (m *MongoPBM) CheckRestore(bcpName string, waitFor time.Duration) error {
	// just to be sure the check hasn't started before the lock were created
	time.Sleep(1 * time.Second)

	tmr := time.NewTimer(waitFor)
	tkr := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-tmr.C:
			return errors.Errorf("timeout reached")
		case <-tkr.C:
			lock, err := m.p.GetLockData(&pbm.LockHeader{
				Type:       pbm.CmdRestore,
				BackupName: bcpName,
			})
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
