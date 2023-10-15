package sdk

import (
	"context"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/internal/ctrl"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/log"
	"github.com/percona/percona-backup-mongodb/internal/topo"
)

var errMissedClusterTime = errors.New("missed cluster time")

func IsHeartbeatStale(clusterTime, other Timestamp) bool {
	return clusterTime.T >= other.T+defs.StaleFrameSec
}

func GetClusterTime(ctx context.Context, m *mongo.Client) (Timestamp, error) {
	info, err := topo.GetNodeInfo(ctx, m)
	if err != nil {
		return primitive.Timestamp{}, err
	}
	if info.ClusterTime == nil {
		return primitive.Timestamp{}, errMissedClusterTime
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

	outC, errC := log.Follow(ctx, c.(*clientImpl).conn.LogCollection(), r, false)

	for {
		select {
		case entry := <-outC:
			if entry != nil && entry.Msg == "succeed" {
				return nil
			}
		case err := <-errC:
			return err
		}
	}
}
