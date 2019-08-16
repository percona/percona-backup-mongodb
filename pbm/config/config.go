package config

type C struct {
	Agents    []Agent
	CurrentOp Operation
}

type Operation string

const (
	NoOp         Operation = "none"
	BackupOp               = "backup"
	PreBackupOp            = "pre_backup"
	RestoreOp              = "restore"
	PreRestoreOp           = "pre_restore"
)

type Agent struct {
	ID   string
	Host string
}

type (
	NodeType string
	NodeRole string
)

const (
	NodeReplset   NodeType = "REPLSET"
	NodeShardSrv           = "SHARDSRV"
	NodeConfigSrv          = "CONFIGSRV"
	NodeMongos             = "MONGOS"

	NodePrimary   NodeRole = "Primary"
	NodeSecondary          = "Secondary"
)

type NodeStatus struct {
}

type Node struct {
	ID        string
	ReplsetID string
	Type      NodeType
	Role      NodeRole
	Status    NodeStatus
}

type Cluster struct {
	Name     string
	ReplSets []ReplSet
}

type ReplSet struct {
	ID    string
	Nodes []Node
}
