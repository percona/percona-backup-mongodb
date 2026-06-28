package pbm

import (
	"github.com/percona/percona-backup-mongodb/x/pbm/disco"
	"github.com/percona/percona-backup-mongodb/x/pbm/etcd"
)

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

	disco.Config
}
