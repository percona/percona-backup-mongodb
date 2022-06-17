package client

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/errgroup"
)

type NodeStatus struct {
	Host       string `json:"host"`
	Ver        string `json:"agent"`
	OK         bool   `json:"ok"`
	Err        error  `json:"err,omitempty"`
	HBErr      error  `json:"hb,omitempty"`
	PBMErr     error  `json:"pbm,omitempty"`
	NodeErr    error  `json:"node,omitempty"`
	StorageErr error  `json:"storage,omitempty"`
}

type errStale struct {
	lastHB primitive.Timestamp
}

func (e errStale) Error() string {
	return fmt.Sprintf("lost agent, last heartbeat: %v", e.lastHB.T)
}

const AppNameCtxKey = "pbm:app:name"

func clusterTime(ctx context.Context, m *mongo.Client) (primitive.Timestamp, error) {
	var rv primitive.Timestamp

	// Make a read to force the cluster timestamp update.
	// Otherwise, cluster timestamp could remain the same between node info reads,
	// while in fact time has been moved forward.
	err := coll(m, pbm.LockCollection).FindOne(ctx, bson.M{}).Err()
	if err != nil && err != mongo.ErrNoDocuments {
		return rv, errors.WithMessage(err, "void read")
	}

	info, err := hello(ctx, m)
	if err != nil {
		return rv, errors.WithMessage(err, "get node info")
	}

	if info.ClusterTime == nil {
		return rv, errors.WithMessage(err, "no clusterTime in response")
	}

	return info.ClusterTime.ClusterTime, nil
}

func hello(ctx context.Context, m *mongo.Client) (*pbm.NodeInfo, error) {
	// TODO: "hello" since 5.0 (and 4.4.2, 4.2.10, 4.0.21, and 3.6.21)
	res := runMCmd(ctx, m, "isMaster")
	if err := res.Err(); err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	var info *pbm.NodeInfo
	return info, errors.WithMessage(res.Decode(info), "decode")
}

type (
	RSName        = string
	NodeName      = string
	ClusterStatus = map[RSName]map[NodeName]NodeStatus
)

func clusterStatus(ctx context.Context, m *mongo.Client) (ClusterStatus, error) {
	allShards, err := clusterShards(ctx, m)
	if err != nil {
		return nil, err
	}

	var allClusterMembers *map[string][]pbm.NodeStatus
	var allAgents *[]pbm.AgentStat

	grp, grpctx := errgroup.WithContext(ctx)
	grp.Go(func() (err error) {
		*allClusterMembers, err = clusterMembers(grpctx, allShards)
		return
	})
	grp.Go(func() (err error) {
		*allAgents, err = agentsStatuses(grpctx, m)
		return
	})
	if err := grp.Wait(); err != nil {
		return nil, err
	}

	ct, err := clusterTime(ctx, m)
	if err != nil {
		return nil, err
	}

	agentsByRS := groupBy(*allAgents,
		func(a *pbm.AgentStat) string { return a.RS })

	rv := make(ClusterStatus, len(allShards))
	for rsName, rsMembers := range *allClusterMembers {
		shardAgentsByNode := keyBy(agentsByRS[rsName],
			func(a *pbm.AgentStat) string { return a.Node })

		nodes := make(map[string]NodeStatus, len(agentsByRS))
		for _, n := range rsMembers {
			node := NodeStatus{
				Host: rsName + "/" + n.Name,
				OK:   true,
			}

			agent, ok := shardAgentsByNode[n.Name]
			if !ok {
				node.OK = false
				node.Ver = "N/A"
				node.Err = errors.New("not found")
				nodes[n.Name] = node
				continue
			}
			node.Ver = "v" + agent.Ver

			if agent.Err != "" {
				node.OK = false
				node.Err = errors.New(agent.Err)
			}
			if agent.Heartbeat.T+pbm.StaleFrameSec < ct.T {
				node.OK = false
				node.HBErr = errStale{agent.Heartbeat}
			}
			if !agent.PBMStatus.OK {
				node.OK = false
				node.PBMErr = errors.New(agent.PBMStatus.Err)
			}
			if !agent.NodeStatus.OK {
				node.OK = false
				node.NodeErr = errors.New(agent.NodeStatus.Err)
			}
			if !agent.StorageStatus.OK {
				node.OK = false
				node.StorageErr = errors.New(agent.StorageStatus.Err)
			}

			nodes[n.Name] = node
		}

		rv[rsName] = nodes
	}

	return rv, nil
}

func clusterShards(ctx context.Context, m *mongo.Client) ([]pbm.Shard, error) {
	info, err := hello(ctx, m)
	if err != nil {
		return nil, err
	}

	selfShard := pbm.Shard{
		RS:   info.SetName,
		Host: info.SetName + "/" + strings.Join(info.Hosts, ","),
	}

	if !info.IsSharded() {
		return []pbm.Shard{selfShard}, nil
	}

	cur, err := m.Database("config").Collection("shards").Find(ctx, bson.M{})
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}

	var shards []pbm.Shard
	if err := cur.All(ctx, &shards); err != nil {
		return nil, err
	}
	shards = append([]pbm.Shard{selfShard}, shards...)

	// https://jira.percona.com/browse/PBM-595
	for i := range shards {
		s := &shards[i]
		if h, _, ok := strings.Cut(s.Host, "/"); ok {
			s.RS = h
		} else {
			s.RS = s.ID
		}
	}

	return shards, nil
}

func clusterMembers(ctx context.Context, shards []pbm.Shard) (map[string][]pbm.NodeStatus, error) {
	rv := make(map[string][]pbm.NodeStatus, len(shards))

	mu := sync.Mutex{}
	grp, grpctx := errgroup.WithContext(ctx)
	for i := range shards {
		s := &shards[i]
		grp.Go(func() error {
			members, err := replSetMembers(grpctx, s.Host)
			if err != nil {
				return err
			}

			mu.Lock()
			rv[s.RS] = members
			mu.Unlock()

			return nil
		})
	}

	return rv, grp.Wait()
}

func replSetMembers(ctx context.Context, uri string) ([]pbm.NodeStatus, error) {
	uri, err := lookupLeaderURI(ctx, uri)
	if err != nil {
		return nil, err
	}

	m, err := connect(ctx, uri)
	if err != nil {
		return nil, err
	}
	defer m.Disconnect(context.Background())

	status, err := replSetStatus(ctx, m)
	if err != nil {
		return nil, err
	}

	return status.Members, nil
}

func replSetStatus(ctx context.Context, m *mongo.Client) (*pbm.ReplsetStatus, error) {
	var status *pbm.ReplsetStatus
	err := runMCmd(ctx, m, "replSetGetStatus").Decode(status)
	return status, errors.Wrap(err, "run mongo command replSetGetStatus")
}

func agentsStatuses(ctx context.Context, m *mongo.Client) ([]pbm.AgentStat, error) {
	cur, err := coll(m, pbm.AgentsStatusCollection).Find(ctx, bson.D{})
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}

	var s []pbm.AgentStat
	err = cur.All(ctx, &s)
	return s, errors.Wrap(err, "cur")
}
