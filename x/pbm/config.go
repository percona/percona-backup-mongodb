package pbm

// CtrlAgentConfig holds agents configuration mostly defined specified using cli or external cfg.
type CtrlAgentConfig struct {
	WorkerAgentConfig
	EtcdConfig

	HTTPPort int
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

// EtcdConfig holds embedded etcd server configuration.
type EtcdConfig struct {
	DataDir string

	// agent listen etcd peer on this port
	ListenPeerPort int
	// agent listen etcd client on this port
	ListenClientPort int

	// agent advertise this url for communication with peer, empty keeps the localhost default.
	AdvertisePeerURL string
	// agent advertise this url for communication with client, empty keeps the localhost default.
	AdvertiseClientURL string

	// InitialCluster is the comma-separated bootstrap member list:
	//   "etcd-0=http://etcd-0...:2380,etcd-1=http://etcd-1...:2380,..."
	// When empty it is derived from the member name and AdvertisePeerURL,
	// yielding a single-node cluster.
	InitialCluster string
}

// DiscoConfig holds Serf discovery configuration shared by all agents.
type DiscoConfig struct {
	SerfPort int
	SerfJoin string
}
