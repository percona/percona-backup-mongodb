package sdk

import (
	"context"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
	"github.com/percona/percona-backup-mongodb/pbm/topo"
)

type (
	ReplsetInfo = topo.Shard
	AgentStatus = topo.AgentStat
)

var (
	ErrMissedClusterTime       = errors.New("missed cluster time")
	ErrInvalidDeleteBackupType = backup.ErrInvalidDeleteBackupType
)

func IsHeartbeatStale(clusterTime, other Timestamp) bool {
	return clusterTime.T >= other.T+defs.StaleFrameSec
}

func ClusterTime(ctx context.Context, client *Client) (Timestamp, error) {
	info, err := topo.GetNodeInfo(ctx, client.conn.MongoClient())
	if err != nil {
		return primitive.Timestamp{}, err
	}
	if info.ClusterTime == nil {
		return primitive.Timestamp{}, ErrMissedClusterTime
	}

	return info.ClusterTime.ClusterTime, nil
}

// ClusterMembers returns list of replsets in the cluster.
//
// For sharded cluster: the configsvr (with ID `config`) and all shards.
// For non-sharded cluster: the replset.
func ClusterMembers(ctx context.Context, client *Client) ([]ReplsetInfo, error) {
	shards, err := topo.ClusterMembers(ctx, client.conn.MongoClient())
	if err != nil {
		return nil, errors.Wrap(err, "topo")
	}
	return shards, nil
}

// AgentStatuses returns list of all PBM Agents statuses.
func AgentStatuses(ctx context.Context, client *Client) ([]AgentStatus, error) {
	return topo.ListAgents(ctx, client.conn)
}

func WaitForResync(ctx context.Context, c *Client, cid CommandID) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	r := &log.LogRequest{
		RecordAttrs: log.RecordAttrs{
			Event: string(ctrl.CmdResync),
			OPID:  string(cid),
			Level: log.InfoLevel,
		},
	}

	outC, errC := log.Follow(ctx, c.conn, r, false)

	for {
		select {
		case entry := <-outC:
			if entry == nil {
				continue
			}
			if entry.Msg == "succeed" {
				return nil
			}
			if entry.Level == log.ErrorLevel {
				return errors.New(entry.Msg)
			}
		case err := <-errC:
			return err
		}
	}
}
