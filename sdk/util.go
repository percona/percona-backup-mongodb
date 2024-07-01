package sdk

import (
	"context"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
)

var (
	ErrMissedClusterTime       = errors.New("missed cluster time")
	ErrInvalidDeleteBackupType = backup.ErrInvalidDeleteBackupType
)

func ParseDeleteBackupType(s string) (BackupType, error) {
	return backup.ParseDeleteBackupType(s)
}

func IsHeartbeatStale(clusterTime, other Timestamp) bool {
	return clusterTime.T >= other.T+defs.StaleFrameSec
}

func GetClusterTime(ctx context.Context, m *mongo.Client) (Timestamp, error) {
	info, err := topo.GetNodeInfo(ctx, m)
	if err != nil {
		return primitive.Timestamp{}, err
	}
	if info.ClusterTime == nil {
		return primitive.Timestamp{}, ErrMissedClusterTime
	}

	return info.ClusterTime.ClusterTime, nil
}

func WaitForResync(ctx context.Context, c Client, cid CommandID) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	r := &log.LogRequest{
		LogKeys: log.LogKeys{
			Event:    string(ctrl.CmdResync),
			OPID:     string(cid),
			Severity: log.Info,
		},
	}

	outC, errC := log.Follow(ctx, c.(*clientImpl).conn, r, false)

	for {
		select {
		case entry := <-outC:
			if entry == nil {
				continue
			}
			if entry.Msg == "succeed" {
				return nil
			}
			if entry.Severity == log.Error {
				return errors.New(entry.Msg)
			}
		case err := <-errC:
			return err
		}
	}
}

func CanDeleteBackup(ctx context.Context, sc Client, bcp *BackupMetadata) error {
	return backup.CanDeleteBackup(ctx, sc.(*clientImpl).conn, bcp)
}

func CanDeleteIncrementalBackup(
	ctx context.Context,
	sc Client,
	bcp *BackupMetadata,
	increments [][]*BackupMetadata,
) error {
	return backup.CanDeleteIncrementalChain(ctx, sc.(*clientImpl).conn, bcp, increments)
}

func ListDeleteBackupBefore(
	ctx context.Context,
	sc Client,
	ts primitive.Timestamp,
	bcpType BackupType,
) ([]BackupMetadata, error) {
	return backup.ListDeleteBackupBefore(ctx, sc.(*clientImpl).conn, ts, bcpType)
}

func ListDeleteChunksBefore(
	ctx context.Context,
	sc Client,
	ts primitive.Timestamp,
) ([]OplogChunk, error) {
	r, err := backup.MakeCleanupInfo(ctx, sc.(*clientImpl).conn, ts)
	return r.Chunks, err
}
