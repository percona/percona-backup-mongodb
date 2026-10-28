// PBM 2.x package
package topo

import (
	"context"
	"fmt"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-backup-mongodb/x/pbm/defs"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/version"
)

type ConnectionStatus struct {
	AuthInfo AuthInfo `bson:"authInfo" json:"authInfo"`
}

type AuthInfo struct {
	Users     []AuthUser      `bson:"authenticatedUsers" json:"authenticatedUsers"`
	UserRoles []AuthUserRoles `bson:"authenticatedUserRoles" json:"authenticatedUserRoles"`
}

type AuthUser struct {
	User string `bson:"user" json:"user"`
	DB   string `bson:"db" json:"db"`
}

type AuthUserRoles struct {
	Role string `bson:"role" json:"role"`
	DB   string `bson:"db" json:"db"`
}

type (
	ReplsetName = string
	ShardName   = string
	NodeURI     = string
)

type topoCheckError struct {
	Replsets map[ReplsetName]map[NodeURI][]error
	Missed   []string
}

func (r topoCheckError) hasError() bool {
	return len(r.Missed) != 0
}

func (r topoCheckError) Error() string {
	if !r.hasError() {
		return ""
	}

	return fmt.Sprintf("no available agent(s) on replsets: %s", strings.Join(r.Missed, ", "))
}

// NodeSuits checks if node can perform backup
func NodeSuits(ctx context.Context, m *mongo.Client, inf *NodeInfo) (bool, error) {
	status, err := GetNodeStatus(ctx, m, inf.Me)
	if err != nil {
		return false, errors.Wrap(err, "get node status")
	}
	if status.IsArbiter() {
		return false, nil
	}

	replLag, err := ReplicationLag(ctx, m, inf.Me)
	if err != nil {
		return false, errors.Wrap(err, "get node replication lag")
	}

	return replLag < defs.MaxReplicationLagTimeSec && status.Health == defs.NodeHealthUp &&
			(status.State == defs.NodeStatePrimary || status.State == defs.NodeStateSecondary),
		nil
}

func NodeSuitsExt(ctx context.Context, m *mongo.Client, inf *NodeInfo, t defs.BackupType) (bool, error) {
	if ok, err := NodeSuits(ctx, m, inf); err != nil || !ok {
		return false, err
	}

	ver, err := version.GetMongoVersion(ctx, m)
	if err != nil {
		return false, errors.Wrap(err, "get mongo version")
	}

	err = version.FeatureSupport(ver).BackupType(t)
	return err == nil, err
}

// GetReplsetStatus returns `replSetGetStatus` for the given connection
func GetReplsetStatus(ctx context.Context, m *mongo.Client) (*ReplsetStatus, error) {
	status := &ReplsetStatus{}
	err := m.Database("admin").RunCommand(ctx, bson.D{{"replSetGetStatus", 1}}).Decode(status)
	if err != nil {
		return nil, errors.Wrap(err, "query adminCommand: replSetGetStatus")
	}

	return status, nil
}

func GetNodeStatus(ctx context.Context, m *mongo.Client, name string) (*NodeStatus, error) {
	s, err := GetReplsetStatus(ctx, m)
	if err != nil {
		return nil, errors.Wrap(err, "get replset status")
	}

	for _, m := range s.Members {
		if m.Name == name {
			return &m, nil
		}
	}

	return nil, errors.ErrNotFound
}

// ReplicationLag returns node replication lag in seconds
func ReplicationLag(ctx context.Context, m *mongo.Client, self string) (int, error) {
	s, err := GetReplsetStatus(ctx, m)
	if err != nil {
		return -1, errors.Wrap(err, "get replset status")
	}

	var primaryOptime, nodeOptime int
	for _, m := range s.Members {
		if m.Name == self {
			nodeOptime = int(m.Optime.TS.T)
		}
		if m.StateStr == "PRIMARY" {
			primaryOptime = int(m.Optime.TS.T)
		}
	}

	return primaryOptime - nodeOptime, nil
}
