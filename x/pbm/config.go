package pbm

import "github.com/percona/percona-backup-mongodb/x/pbm/etcd"

// CtrlAgentConfig holds agents configuration mostly defined specified using cli or external cfg.
type CtrlAgentConfig struct {
	WorkerAgentConfig
	etcd.Config

	APISrvPort int
}

type WorkerAgentConfig struct {
	// Every agent should have unique name in the cluster.
	// It can be inferred from node name if connection string is specified and PSMDB is available.
	// Or it can be just predefined unique name.
	// It'll be used to identify the agent as well as etcd instance in case of ctrl-agents.
	Name     string
	MongoURI string

	DiscoConfig
}

// DiscoConfig holds Serf discovery configuration shared by all agents.
type DiscoConfig struct {
	SerfPort int
	// SerfJoin lists seed addresses (host:port) to gossip with on startup.
	// Reaching any one is enough; serf discovers the rest. Empty starts a new
	// cluster.
	SerfJoin []string
}
