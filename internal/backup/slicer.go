package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/oplog"
	"github.com/percona/percona-backup-mongodb/internal/topo"
)

type uploadChunkFunc func(ctx context.Context, w io.WriterTo, from, till primitive.Timestamp) (int64, error)

type stopSlicerFunc func() (primitive.Timestamp, int64, error)

func startOplogSlicer(
	ctx context.Context,
	m *mongo.Client,
	interval time.Duration,
	startOpTime primitive.Timestamp,
	upload uploadChunkFunc,
) stopSlicerFunc {
	l := log.LogEventFromContext(ctx)
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

				l.Error("failed to get last write: %v", err)
				continue
			}
			if !currOpTime.After(startOpTime) {
				continue
			}

			oplog.SetTailingSpan(startOpTime, currOpTime)

			var n int64
			n, err = upload(ctx, oplog, startOpTime, currOpTime)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}

				l.Error("failed to upload oplog: %v", err)
				continue
			}

			logm := fmt.Sprintf("created chunk %s - %s",
				time.Unix(int64(startOpTime.T), 0).UTC().Format("2006-01-02T15:04:05"),
				time.Unix(int64(currOpTime.T), 0).UTC().Format("2006-01-02T15:04:05"))
			if keepRunning {
				nextChunkT := time.Now().Add(interval)
				logm += fmt.Sprintf(". Next chunk creation scheduled to begin at ~%s", nextChunkT)
			}
			l.Info(logm)

			startOpTime = currOpTime
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

		return startOpTime, uploaded, err
	}
}
