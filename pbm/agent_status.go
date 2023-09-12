package pbm

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type AgentStat struct {
	Node          string              `bson:"n"`
	RS            string              `bson:"rs"`
	State         NodeState           `bson:"s"`
	StateStr      string              `bson:"str"`
	Hidden        bool                `bson:"hdn"`
	Passive       bool                `bson:"psv"`
	Ver           string              `bson:"v"`
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

func (p *PBM) SetAgentStatus(stat AgentStat) error {
	ct, err := p.ClusterTime()
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}
	stat.Heartbeat = ct

	_, err = p.Conn.Database(DB).Collection(AgentsStatusCollection).ReplaceOne(
		p.ctx,
		bson.D{{"n", stat.Node}, {"rs", stat.RS}},
		stat,
		options.Replace().SetUpsert(true),
	)
	return errors.Wrap(err, "write into db")
}

func (p *PBM) RemoveAgentStatus(stat AgentStat) error {
	_, err := p.Conn.Database(DB).Collection(AgentsStatusCollection).
		DeleteOne(p.ctx, bson.D{{"n", stat.Node}, {"rs", stat.RS}})
	return errors.WithMessage(err, "query")
}

// GetAgentStatus returns agent status by given node and rs
// it's up to user how to handle ErrNoDocuments
func (p *PBM) GetAgentStatus(rs, node string) (AgentStat, error) {
	res := p.Conn.Database(DB).Collection(AgentsStatusCollection).FindOne(
		p.ctx,
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
func (p *PBM) AgentStatusGC() error {
	ct, err := p.ClusterTime()
	if err != nil {
		return errors.Wrap(err, "get cluster time")
	}
	// 30 secs is the connection time out for mongo. So if there are some connection issues the agent checker
	// may stuck for 30 sec on ping (trying to connect), it's HB became stale and it would be collected.
	// Which would lead to the false clamin "not found" in the status output. So stale range should at least 30 sec
	// (+5 just in case).
	// XXX: stalesec is const 15 secs which resolves to 35 secs
	stalesec := AgentsStatCheckRange.Seconds() * 3
	if stalesec < 35 {
		stalesec = 35
	}
	ct.T -= uint32(stalesec)
	_, err = p.Conn.Database(DB).Collection(AgentsStatusCollection).DeleteMany(
		p.ctx,
		bson.M{"hb": bson.M{"$lt": ct}},
	)

	return errors.Wrap(err, "delete")
}

// ListAgentStatuses returns list of registered agents
func (p *PBM) ListAgentStatuses() ([]AgentStat, error) {
	if err := p.AgentStatusGC(); err != nil {
		return nil, errors.WithMessage(err, "remove stale statuses")
	}

	cur, err := p.Conn.Database(DB).Collection(AgentsStatusCollection).Find(p.ctx, bson.M{})
	if err != nil {
		return nil, errors.WithMessage(err, "query")
	}

	var agents []AgentStat
	err = cur.All(p.ctx, &agents)
	return agents, errors.WithMessage(err, "decode")
}

// GetReplsetStatus returns `replSetGetStatus` for the replset
// or config server in case of sharded cluster
func (p *PBM) GetReplsetStatus() (*ReplsetStatus, error) {
	return GetReplsetStatus(p.ctx, p.Conn)
}

// GetReplsetStatus returns `replSetGetStatus` for the given connection
func GetReplsetStatus(ctx context.Context, cn *mongo.Client) (*ReplsetStatus, error) {
	status := &ReplsetStatus{}
	err := cn.Database("admin").RunCommand(ctx, bson.D{{"replSetGetStatus", 1}}).Decode(status)
	if err != nil {
		return nil, errors.WithMessage(err, "query adminCommand: replSetGetStatus")
	}

	return status, nil
}
