package topo

import (
	"context"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	"github.com/percona/percona-backup-mongodb/pbm/connect"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

// Shard represent config.shard https://docs.mongodb.com/manual/reference/config-database/#config.shards
// _id may differ from the rs name, so extract rs name from the host (format like "rs2/localhost:27017")
// see https://jira.percona.com/browse/PBM-595
type Shard struct {
	ID   string `bson:"_id"`
	RS   string `bson:"-"`
	Host string `bson:"host"`
}

// ClusterTime returns mongo's current cluster time
func GetClusterTime(ctx context.Context, m connect.Client) (primitive.Timestamp, error) {
	// Make a read to force the cluster timestamp update.
	// Otherwise, cluster timestamp could remain the same between node info reads,
	// while in fact time has been moved forward.
	err := m.LockCollection().FindOne(ctx, bson.D{}).Err()
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return primitive.Timestamp{}, errors.Wrap(err, "void read")
	}

	inf, err := GetNodeInfo(ctx, m.MongoClient())
	if err != nil {
		return primitive.Timestamp{}, errors.Wrap(err, "get NodeInfo")
	}

	return ClusterTimeFromNodeInfo(inf)
}

func ClusterTimeFromNodeInfo(info *NodeInfo) (primitive.Timestamp, error) {
	if info.ClusterTime == nil {
		return primitive.Timestamp{}, errors.New("no clusterTime in response")
	}

	return info.ClusterTime.ClusterTime, nil
}

func GetLastWrite(ctx context.Context, m *mongo.Client, majority bool) (primitive.Timestamp, error) {
	inf, err := GetNodeInfo(ctx, m)
	if err != nil {
		return primitive.Timestamp{}, errors.Wrap(err, "get NodeInfo data")
	}
	return OpTimeFromNodeInfo(inf, majority)
}

func OpTimeFromNodeInfo(inf *NodeInfo, majority bool) (primitive.Timestamp, error) {
	lw := inf.LastWrite.MajorityOpTime.TS
	if !majority {
		lw = inf.LastWrite.OpTime.TS
	}
	if lw.T == 0 {
		return primitive.Timestamp{}, errors.New("last write timestamp is nil")
	}
	return lw, nil
}

// IsWriteMajorityRequested compares cluster wide majority (replSetGetStatus.writeMajorityCount)
// with WriteConcern requested in connection string and determinates if majority is requested or not
func IsWriteMajorityRequested(
	ctx context.Context,
	m *mongo.Client,
	writeConcern *writeconcern.WriteConcern,
) (bool, error) {
	if writeConcern == nil ||
		!writeConcern.IsValid() ||
		writeConcern == writeconcern.Majority() {
		return true, nil
	}

	w, ok := writeConcern.W.(int)
	if !ok {
		return true, nil
	}

	s, err := GetReplsetStatus(ctx, m)
	if err != nil {
		return true, errors.Wrap(err, "get replset status")
	}

	return w >= s.WriteMajorityCount,
		nil
}

// ClusterMembers returns list of replicasets current cluster consists of
// (shards + configserver). The list would consist of on rs if cluster is
// a non-sharded rs.
func ClusterMembers(ctx context.Context, m *mongo.Client) ([]Shard, error) {
	// it would be a config server in sharded cluster
	inf, err := GetNodeInfo(ctx, m)
	if err != nil {
		return nil, errors.Wrap(err, "define cluster state")
	}

	var shards []Shard
	if inf.IsMongos() || inf.IsSharded() {
		members, err := getShardMapImpl(ctx, m)
		if err != nil {
			return nil, err
		}

		shards = make([]Shard, 0, len(members))
		for _, v := range members {
			shards = append(shards, v)
		}

		return shards, nil
	}

	shards = []Shard{{
		ID:   inf.SetName,
		RS:   inf.SetName,
		Host: inf.SetName + "/" + strings.Join(inf.Hosts, ","),
	}}
	return shards, nil
}

func getShardMapImpl(ctx context.Context, m *mongo.Client) (map[ReplsetName]Shard, error) {
	res := m.Database("admin").RunCommand(ctx, bson.D{{"getShardMap", 1}})
	if err := res.Err(); err != nil {
		return nil, errors.Wrap(err, "query")
	}

	// the map field is mapping of shard names to replset uri
	// if shard name is not set, mongodb will provide unique name for it
	// (e.g. the replset name of the shard)
	// for configsvr, key name is "config"
	var shardMap struct{ Map map[string]string }
	if err := res.Decode(&shardMap); err != nil {
		return nil, errors.Wrap(err, "decode")
	}

	shards := make(map[string]Shard, len(shardMap.Map))
	for id, host := range shardMap.Map {
		rs, _, _ := strings.Cut(host, "/")
		shards[rs] = Shard{
			ID:   id,
			RS:   rs,
			Host: host,
		}
	}

	return shards, nil
}

type BalancerMode string

const (
	BalancerModeOn  BalancerMode = "full"
	BalancerModeOff BalancerMode = "off"
)

func (m BalancerMode) String() string {
	switch m {
	case BalancerModeOn:
		return "on"
	case BalancerModeOff:
		return "off"
	default:
		return "unknown"
	}
}

type BalancerStatus struct {
	Mode              BalancerMode `bson:"mode" json:"mode"`
	InBalancerRound   bool         `bson:"inBalancerRound" json:"inBalancerRound"`
	NumBalancerRounds int64        `bson:"numBalancerRounds" json:"numBalancerRounds"`
	Ok                int          `bson:"ok" json:"ok"`
}

func (b *BalancerStatus) IsOn() bool {
	return b.Mode == BalancerModeOn
}

// SetBalancerStatus sets balancer status
func SetBalancerStatus(ctx context.Context, m connect.Client, mode BalancerMode) error {
	var cmd string

	switch mode {
	case BalancerModeOn:
		cmd = "_configsvrBalancerStart"
	case BalancerModeOff:
		cmd = "_configsvrBalancerStop"
	default:
		return errors.Errorf("unknown mode %s", mode)
	}

	err := m.AdminCommand(ctx, bson.D{{cmd, 1}}).Err()
	if err != nil {
		return errors.Wrap(err, "run mongo command")
	}
	return nil
}

// GetBalancerStatus returns balancer status
func GetBalancerStatus(ctx context.Context, m connect.Client) (*BalancerStatus, error) {
	inf := &BalancerStatus{}
	err := m.AdminCommand(ctx, bson.D{{"_configsvrBalancerStatus", 1}}).Decode(inf)
	if err != nil {
		return nil, errors.Wrap(err, "run mongo command")
	}
	return inf, nil
}
