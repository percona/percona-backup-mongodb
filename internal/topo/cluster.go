package topo

import (
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/errors"
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
func GetClusterTime(ctx context.Context, m connect.MetaClient) (primitive.Timestamp, error) {
	// Make a read to force the cluster timestamp update.
	// Otherwise, cluster timestamp could remain the same between node info reads,
	// while in fact time has been moved forward.
	err := m.LockCollection().FindOne(ctx, bson.D{}).Err()
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return primitive.Timestamp{}, errors.Wrap(err, "void read")
	}

	inf, err := GetNodeInfoExt(ctx, m.UnsafeClient())
	if err != nil {
		return primitive.Timestamp{}, errors.Wrap(err, "get NodeInfo")
	}

	if inf.ClusterTime == nil {
		return primitive.Timestamp{}, errors.Wrap(err, "no clusterTime in response")
	}

	return inf.ClusterTime.ClusterTime, nil
}

func GetLastWrite(ctx context.Context, m *mongo.Client, majority bool) (primitive.Timestamp, error) {
	inf, err := GetNodeInfo(ctx, m)
	if err != nil {
		return primitive.Timestamp{}, errors.Wrap(err, "get NodeInfo data")
	}
	lw := inf.LastWrite.MajorityOpTime.TS
	if !majority {
		lw = inf.LastWrite.OpTime.TS
	}
	if lw.T == 0 {
		return primitive.Timestamp{}, errors.New("last write timestamp is nil")
	}
	return lw, nil
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

	if inf.IsMongos() || inf.IsSharded() {
		return getClusterMembersImpl(ctx, m)
	}

	shards := []Shard{{
		RS:   inf.SetName,
		Host: inf.SetName + "/" + strings.Join(inf.Hosts, ","),
	}}
	return shards, nil
}

func getClusterMembersImpl(ctx context.Context, m *mongo.Client) ([]Shard, error) {
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

	shards := make([]Shard, 0, len(shardMap.Map))
	for id, host := range shardMap.Map {
		if id == "<local>" || strings.ContainsAny(id, "/:") {
			// till 4.2, map field is like connStrings (added in 4.4)
			// and <local> key is uri of the directly (w/o mongos) connected replset
			// skip not shard name
			continue
		}

		rs, _, _ := strings.Cut(host, "/")
		shards = append(shards, Shard{
			ID:   id,
			RS:   rs,
			Host: host,
		})
	}

	return shards, nil
}
