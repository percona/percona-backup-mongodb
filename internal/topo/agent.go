package topo

import (
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/mod/semver"

	"github.com/percona/percona-backup-mongodb/internal/connect"
	"github.com/percona/percona-backup-mongodb/internal/context"
	"github.com/percona/percona-backup-mongodb/internal/defs"
	"github.com/percona/percona-backup-mongodb/internal/errors"
	"github.com/percona/percona-backup-mongodb/internal/version"
)

type AgentStat struct {
	Node          string              `bson:"n"`
	RS            string              `bson:"rs"`
	State         defs.NodeState      `bson:"s"`
	StateStr      string              `bson:"str"`
	Hidden        bool                `bson:"hdn"`
	Passive       bool                `bson:"psv"`
	Arbiter       bool                `bson:"arb"`
	AgentVer      string              `bson:"v"`
	MongoVer      string              `bson:"mv"`
	PerconaVer    string              `bson:"pv,omitempty"`
	PBMStatus     SubsysStatus        `bson:"pbms"`
	NodeStatus    SubsysStatus        `bson:"nodes"`
	StorageStatus SubsysStatus        `bson:"stors"`
	Heartbeat     primitive.Timestamp `bson:"hb"`
	Err           string              `bson:"e"`
}

type SubsysStatus struct {
	OK  bool   `bson:"ok"`
	Err string `bson:"e"`
}

func (s *AgentStat) OK() (bool, []string) {
	var errs []string
	ok := true
	if !s.PBMStatus.OK {
		ok = false
		errs = append(errs, fmt.Sprintf("PBM connection: %s", s.PBMStatus.Err))
	}
	if !s.NodeStatus.OK {
		ok = false
		errs = append(errs, fmt.Sprintf("node connection: %s", s.NodeStatus.Err))
	}
	if !s.StorageStatus.OK {
		ok = false
		errs = append(errs, fmt.Sprintf("storage: %s", s.StorageStatus.Err))
	}

	return ok, errs
}

func (s *AgentStat) MongoVersion() version.MongoVersion {
	v := version.MongoVersion{
		PSMDBVersion:  s.PerconaVer,
		VersionString: s.MongoVer,
	}

	vs := semver.Canonical("v" + s.MongoVer)[1:]
	vs = strings.SplitN(vs, "-", 2)[0]
	for _, a := range strings.Split(vs, ".")[:3] {
		n, _ := strconv.Atoi(a)
		v.Version = append(v.Version, n)
	}

	return v
}

func SetAgentStatus(ctx context.Context, m connect.MetaClient, stat AgentStat) error {
	ct, err := GetClusterTime(ctx, m)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}
	stat.Heartbeat = ct

	_, err = m.AgentsStatusCollection().ReplaceOne(
		ctx,
		bson.D{{"n", stat.Node}, {"rs", stat.RS}},
		stat,
		options.Replace().SetUpsert(true),
	)
	return errors.Wrap(err, "write into db")
}

func RemoveAgentStatus(ctx context.Context, m connect.MetaClient, stat AgentStat) error {
	_, err := m.AgentsStatusCollection().
		DeleteOne(ctx, bson.D{{"n", stat.Node}, {"rs", stat.RS}})
	return errors.Wrap(err, "query")
}

// GetAgentStatus returns agent status by given node and rs
// it's up to user how to handle ErrNoDocuments
func GetAgentStatus(ctx context.Context, m connect.MetaClient, rs, node string) (AgentStat, error) {
	res := m.AgentsStatusCollection().FindOne(
		ctx,
		bson.D{{"n", node}, {"rs", rs}},
	)
	if res.Err() != nil {
		return AgentStat{}, errors.Wrap(res.Err(), "query mongo")
	}

	var s AgentStat
	err := res.Decode(&s)
	return s, errors.Wrap(err, "decode")
}

// AgentStatusGC cleans up stale agent statuses
func AgentStatusGC(ctx context.Context, m connect.MetaClient) error {
	ct, err := GetClusterTime(ctx, m)
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}
	// 30 secs is the connection time out for mongo. So if there are some connection issues the agent checker
	// may stuck for 30 sec on ping (trying to connect), it's HB became stale and it would be collected.
	// Which would lead to the false clamin "not found" in the status output. So stale range should at least 30 sec
	// (+5 just in case).
	// XXX: stalesec is const 15 secs which resolves to 35 secs
	stalesec := defs.AgentsStatCheckRange.Seconds() * 3
	if stalesec < 35 {
		stalesec = 35
	}
	ct.T -= uint32(stalesec)
	_, err = m.AgentsStatusCollection().DeleteMany(
		ctx,
		bson.M{"hb": bson.M{"$lt": ct}},
	)

	return errors.Wrap(err, "delete")
}

// ListAgentStatuses returns list of registered agents
func ListAgentStatuses(ctx context.Context, m connect.MetaClient) ([]AgentStat, error) {
	if err := AgentStatusGC(ctx, m); err != nil {
		return nil, errors.Wrap(err, "remove stale statuses")
	}

	return ListAgents(ctx, m)
}

func ListAgents(ctx context.Context, m connect.MetaClient) ([]AgentStat, error) {
	cur, err := m.AgentsStatusCollection().Find(ctx, bson.M{})
	if err != nil {
		return nil, errors.Wrap(err, "query")
	}

	var agents []AgentStat
	err = cur.All(ctx, &agents)
	return agents, errors.Wrap(err, "decode")
}
