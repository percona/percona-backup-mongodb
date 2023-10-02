package sdk

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/topo"
)

func IsHeartbeatStale(clusterTime, other Timestamp) bool {
	return clusterTime.T >= other.T+defs.StaleFrameSec
}

func GetClusterTime(ctx context.Context, m *mongo.Client) (Timestamp, error) {
	info, err := topo.GetNodeInfo(ctx, m)
	if err != nil {
		return primitive.Timestamp{}, err
	}
	if info.ClusterTime == nil {
		return primitive.Timestamp{}, errors.New("missed cluster time")
	}

	return info.ClusterTime.ClusterTime, nil
}
