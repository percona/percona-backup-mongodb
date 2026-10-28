package connect

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/zap"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
)

// AgentConn holds the agent's resolved name and its MongoDB connections.
//
// NodeConn and LeadConn are both nil when the agent is not attached to a mongod
// (no connection string) or when its mongod is unreachable while the name is
// specified explicitly.
type AgentConn struct {
	// Name is the unique agent name in the cluster.
	Name string
	// NodeConn is a direct connection to the agent's local mongod.
	NodeConn *mongo.Client
	// LeadConn is a connection to the cluster leader.
	LeadConn Client
}

// Disconnect tears down both connections: node and lead.
func (c *AgentConn) Disconnect(ctx context.Context, lg *zap.Logger) {
	if c.LeadConn != nil {
		if err := c.LeadConn.Disconnect(ctx); err != nil {
			lg.Warn("disconnect cluster leader", zap.Error(err))
		}
	}
	Disconnect(lg, c.NodeConn)
}

// ConnectAgent resolves the agent name and connects to the agent's mongod.
//
// If name is empty, it is inferred from mongod (hello.me) and a connection
// failure is returned as an error.
// If name is given, it takes precedence over the mongod name, and an
// unreachable mongod leaves the agent without connections.
// An empty uri means a detached agent and requires a name.
func ConnectAgent(ctx context.Context, lg *zap.Logger, uri, name string) (*AgentConn, error) {
	if uri == "" && name == "" {
		return nil, errors.New("agent name or mongodb connection string must be specified")
	}

	if uri == "" {
		lg.Info(
			"agent is not attached to mongod",
			zap.String("name", name),
		)
		return &AgentConn{Name: name}, nil
	}

	c, err := connectAgent(ctx, uri)
	if err != nil {
		if name == "" {
			return nil, errors.Wrap(err, "infer agent name from mongod")
		}

		lg.Error(
			"mongod is unreachable, agent continues without it",
			zap.String("name", name),
			zap.Error(err),
		)
		return &AgentConn{Name: name}, nil
	}

	if name == "" {
		return c, nil
	}

	if name != c.Name {
		lg.Warn(
			"agent name differs from mongod name, it should be aligned",
			zap.String("name", name),
			zap.String("mongod-name", c.Name),
		)
	}
	c.Name = name
	return c, nil
}

// connectAgent opens both connections and sets Name to the mongod name.
func connectAgent(ctx context.Context, uri string) (*AgentConn, error) {
	nodeConn, err := MongoConnect(ctx, uri, AppName("pbmx-agent"), Direct(true), NoRS())
	if err != nil {
		return nil, errors.Wrap(err, "connect local mongod")
	}

	inf, err := getNodeInfo(ctx, nodeConn)
	if err != nil {
		_ = nodeConn.Disconnect(ctx)
		return nil, errors.Wrap(err, "get local node info")
	}
	if inf.Me == "" {
		_ = nodeConn.Disconnect(ctx)
		return nil, errors.New("mongod did not report its name (hello.me)")
	}

	leadConn, err := Connect(ctx, uri, "pbmx-agent")
	if err != nil {
		_ = nodeConn.Disconnect(ctx)
		return nil, errors.Wrap(err, "connect cluster leader")
	}

	return &AgentConn{
		Name:     inf.Me,
		NodeConn: nodeConn,
		LeadConn: leadConn,
	}, nil
}
