package backup

import (
	"context"
	"errors"
	"io"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/oplog"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
)

type uploadChunkFunc func(ctx context.Context, w io.WriterTo, from, till primitive.Timestamp) (int64, error)

type stopSlicerFunc func() (primitive.Timestamp, int64, error)

func startOplogSlicer(
	ctx context.Context,
	m *mongo.Client,
	interval time.Duration,
	nextOpTime primitive.Timestamp,
	upload uploadChunkFunc,
) stopSlicerFunc {
	var uploaded int64
	var err error

	stopSignalC := make(chan struct{})
	stoppedC := make(chan struct{})

	go func() {
		defer close(stoppedC)

		t := time.NewTicker(interval)
		defer t.Stop()

		oplog := oplog.NewOplogBackup(m)
		keepRunning := true
		for keepRunning {
			select {
			case <-t.C:
				// tick. do slicing
			case <-stopSignalC:
				// this is the last slice
				keepRunning = false
			case <-ctx.Done():
				err = ctx.Err()
				return
			}

			currOpTime, err := topo.GetLastWrite(ctx, m, true)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}

				log.LogEventFromContext(ctx).Error("failed to get last write: %v", err)
				continue
			}
			if !currOpTime.After(nextOpTime) {
				continue
			}

			oplog.SetTailingSpan(nextOpTime, currOpTime)

			var n int64
			n, err = upload(ctx, oplog, nextOpTime, currOpTime)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}

				log.LogEventFromContext(ctx).Error("failed to upload oplog: %v", err)
				continue
			}

			nextOpTime = currOpTime
			uploaded += n
		}
	}()

	return func() (primitive.Timestamp, int64, error) {
		select {
		case <-stoppedC:
			// already closed
		default:
			close(stopSignalC)
			<-stoppedC
		}

		return nextOpTime, uploaded, err
	}
}
