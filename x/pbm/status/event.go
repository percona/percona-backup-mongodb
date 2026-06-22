package status

// EventKind classifies a transport-level membership change.
type EventKind int

const (
	// MemberUp is a member joining the cluster
	MemberUp EventKind = iota
	// MemberUpdate is a member whose broadcast changes
	MemberUpdate
	// MemberFailed is a member that became unreachable
	MemberFailed
	// MemberGone is a member that intentionally left or was reaped.
	MemberGone
)

// StatusEvent is a transport-level membership change handed from the discovery
// layer to the status service.
type StatusEvent struct {
	Kind    EventKind
	Name    string
	Addr    string
	Port    uint16
	Alive   bool
	Payload map[string]string
}

// Publisher broadcasts this agent's domain tags to the cluster.
type Publisher interface {
	Publish(tags map[string]string) error
}
