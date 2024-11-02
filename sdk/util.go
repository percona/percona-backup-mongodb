package sdk

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm/backup"
	"github.com/percona/percona-backup-mongodb/pbm/ctrl"
	"github.com/percona/percona-backup-mongodb/pbm/defs"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/lock"
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
		LogKeys: log.LogKeys{
			Event:    string(ctrl.CmdResync),
			OPID:     string(cid),
			Severity: log.Info,
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
			if entry.Severity == log.Error {
				return errors.New(entry.Msg)
			}
		case err := <-errC:
			return err
		}
	}
}

func Diagnostic(ctx context.Context, c *Client, cid, dirname string) error {
	if fileInfo, err := os.Stat(dirname); err != nil {
		if !os.IsNotExist(err) {
			return errors.Wrap(err, "stat")
		}
		err = os.MkdirAll(dirname, 0o777)
		if err != nil {
			return errors.Wrap(err, "create path")
		}
	} else if !fileInfo.IsDir() {
		return errors.Errorf("%s is not a dir", dirname)
	}

	opid, err := ctrl.ParseOPID(cid)
	if err != nil {
		return ErrInvalidCommandID
	}

	topology := struct {
		ClusterTime primitive.Timestamp `json:"cluster_time"`
		Members     []topo.Shard        `json:"replsets"`
		Agents      []AgentStatus       `json:"agents"`
	}{}

	topology.ClusterTime, err = topo.GetClusterTime(ctx, c.conn)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}
	topology.Members, err = topo.ClusterMembers(ctx, c.conn.MongoClient())
	if err != nil {
		return errors.Wrap(err, "get members")
	}
	topology.Agents, err = topo.ListAgents(ctx, c.conn)
	if err != nil {
		return errors.Wrap(err, "get agents")
	}
	if err = writeToFile(dirname, "topo.json", topology); err != nil {
		return errors.Wrap(err, "add topo.json")
	}

	locks := struct {
		Locks   []lock.LockData `json:"locks"`
		OpLocks []lock.LockData `json:"op_locks"`
	}{}

	locks.Locks, err = lock.GetLocks(ctx, c.conn, &lock.LockHeader{})
	if err != nil {
		return errors.Wrap(err, "get locks")
	}
	locks.OpLocks, err = lock.GetOpLocks(ctx, c.conn, &lock.LockHeader{})
	if err != nil {
		return errors.Wrap(err, "get op locks")
	}
	if err = writeToFile(dirname, "locks.json", locks); err != nil {
		return errors.Wrap(err, "add locks.json")
	}

	cmd, err := c.CommandInfo(ctx, CommandID(opid.String()))
	if err != nil {
		return errors.Wrap(err, "get command info")
	}
	if err = writeToFile(dirname, "cmd.json", cmd); err != nil {
		return errors.Wrap(err, "add command.json")
	}

	from, err := log.GetFirstTSForOPID(ctx, c.conn, opid.String())
	if err != nil {
		return errors.Wrap(err, "get first opid ts")
	}
	till, err := log.GetLastTSForOPID(ctx, c.conn, opid.String())
	if err != nil {
		return errors.Wrap(err, "get last opid ts")
	}

	logCursor, err := c.conn.LogCollection().Find(ctx,
		bson.D{{"ts", bson.M{"$gte": from, "$lte": till}}})
	if err != nil {
		return errors.Wrap(err, "log: create cursor")
	}

	logs := struct {
		Logs []log.Entry `bson:"logs"`
	}{}
	if err := logCursor.All(ctx, &logs.Logs); err != nil {
		return errors.Wrap(err, "log: cursor %v")
	}
	if err = writeToFile(dirname, "logs.json", logs); err != nil {
		return errors.Wrap(err, "add backup.json")
	}

	switch cmd.Cmd {
	case CmdBackup:
		meta, err := c.GetBackupByOpID(ctx, opid.String(), GetBackupByNameOptions{})
		if err != nil {
			return errors.Wrap(err, "get backup meta")
		}
		if err = writeToFile(dirname, "backup.json", meta); err != nil {
			return errors.Wrap(err, "add backup.json")
		}
	case CmdRestore:
		meta, err := c.GetRestoreByOpID(ctx, opid.String())
		if err != nil {
			return errors.Wrap(err, "get restore meta")
		}
		if err = writeToFile(dirname, "restore.json", meta); err != nil {
			return errors.Wrap(err, "add restore.json")
		}
	}

	return nil
}

func writeToFile(dirname, name string, val any) error {
	data, err := bson.MarshalExtJSON(val, true, true)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	file, err := os.Create(filepath.Join(dirname, name))
	if err != nil {
		return err
	}
	defer file.Close()

	n, err := file.Write(data)
	if err != nil {
		return errors.Wrap(err, "write")
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	err = file.Close()
	if err != nil {
		return errors.Wrap(err, "close file")
	}

	return nil
}
