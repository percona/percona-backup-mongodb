package defs

var (
	replsetName string
	nodeID      string
)

// Replset returns replset name.
func Replset() string {
	return replsetName
}

// Replset returns node id (host:port).
func NodeID() string {
	return nodeID
}

// SetNodeBrief set global replset name and node id [host:port].
//
// NOTE: this should be collect immediately and once
// (the values cannot be changed without mongod restart).
func SetNodeBrief(rs, node string) {
	replsetName = rs
	nodeID = node
}
