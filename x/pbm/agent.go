package pbm

// EtcdConfig holds embedded etcd server configuration.
//
// Leaving a field at its zero value keeps etcd's built-in default, which yields
// a single-node server on localhost (handy for local runs). To form a
// multi-node cluster (typically 1, 3, or 5 members) point the Advertise* URLs
// at routable addresses and list every member in InitialCluster.
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

// AgentConfig holds agents configuration mostly defined specified using cli or cfg.
type AgentConfig struct {
	// Every agent should have unique name in the cluster.
	// It can be inferred from node name if connection string is specified and PSMDB is available.
	// Or it can be just predefined unique name.
	// It'll be used to identify the agent as well as etcd instance in case of ctrl-agents.
	Name string

	EtcdConfig

	HTTPPort int
	MongoURI string
}
