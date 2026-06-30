package status

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-backup-mongodb/x/pbm/topo"
)

// refreshInterval is how often an agent re-reads its local mongod and
// re-broadcasts its own AgentInfo so peers' caches stay converged.
const refreshInterval = 5 * time.Second

// ErrNotFound is returned by GetMember when no member is cached under the name.
var ErrNotFound = errors.New("member not found")

type Role string

const (
	RoleCtrl   Role = "ctrl"
	RoleWorker Role = "worker"
)

// AgentInfo describes a discovered cluster node. It includes mongo properties when
// the agent is attached to a MongoDB instance.
type AgentInfo struct {
	Name      string
	Addr      string
	Port      int
	APIPort   int
	Role      Role
	IsLeader  bool
	Alive     bool
	MongoInfo MongoInfo

	AgentStatus SubsysStatus
	// NodeStatus    SubsysStatus
	// StorageStatus SubsysStatus
}

type MongoInfo struct {
	SetName     string
	Me          string
	Sharded     bool
	ConfigSvr   bool
	IsPrimary   bool
	Secondary   bool
	Hidden      bool
	Passive     bool
	ArbiterOnly bool
	Status      string
}

type SubsysStatus struct {
	OK  bool
	Err string
}

// Svc is the status service. It caches every cluster member's AgentInfo (built from
// StatusEvents received over the discovery layer). Periodically, it reads its own local mongod
// and broadcasts the local AgentInfo to all other peers.
type Svc struct {
	name string // this agent's serf node name
	role Role

	// mc is a direct connection to this agent's local mongod.
	// It's nil when the agent has no attached.
	mc *mongo.Client

	// leader reports whether this agent is the cluster leader
	leader LeaderChecker

	// apiPort is this agent's web API port, advertised to peers so a non-leader
	// can return the leader's API endpoint. Zero for agents with no API (workers).
	apiPort int

	recv ReceiveChannel
	pub  Publisher

	mu   sync.RWMutex
	mems clusterMembers
}

// clusterMembers is the in-memory store of cluster members keyed by node name
type clusterMembers map[string]AgentInfo

// ReceiveChannel carries StatusEvents from discovery
type ReceiveChannel chan StatusEvent

// LeaderChecker reports whether this agent currently holds cluster leadership.
type LeaderChecker interface {
	IsLeader() bool
}

// New creates a status service.
// apiPort is this agent's web API port, advertised to peers, pass 0 for agents
// with no API (workers).
func New(name string, role Role, localMongo *mongo.Client, apiPort int) *Svc {
	return &Svc{
		name:    name,
		role:    role,
		mc:      localMongo,
		apiPort: apiPort,
		recv:    make(ReceiveChannel, 64), // todo: improve
		mems:    map[string]AgentInfo{},
	}
}

// NewForCtrlAgent creates a status service for ctrl-agent.
func NewForCtrlAgent(name string, localMongo *mongo.Client, apiPort int) *Svc {
	return New(name, RoleCtrl, localMongo, apiPort)
}

// NewForWorkerAgent creates a status service for worker-agent.
func NewForWorkerAgent(name string, localMongo *mongo.Client) *Svc {
	return New(name, RoleWorker, localMongo, 0)
}

// DiscoSync exposes the receive channel as a send-only end for the discovery layer.
func (s *Svc) DiscoSync() chan<- StatusEvent {
	return s.recv
}

// SetPublisher wires the broadcast using disco.
func (s *Svc) SetPublisher(p Publisher) {
	s.pub = p
}

// SetLeaderChecker inject leader checker from etcd server.
func (s *Svc) SetLeaderChecker(l LeaderChecker) {
	s.leader = l
}

// IsLeader reports whether this agent currently holds cluster leadership.
func (s *Svc) IsLeader() bool {
	// it can be nil in case of worker-agent
	return s.leader != nil && s.leader.IsLeader()
}

// Run drains membership events into the cache and periodically refreshes and
// broadcasts local node info until ctx is canceled.
func (s *Svc) Run(ctx context.Context) {
	t := time.NewTicker(refreshInterval)
	defer t.Stop()

	s.refreshLocal(ctx)
	for {
		select {
		case ev := <-s.recv:
			s.apply(ev)
		case <-t.C:
			s.refreshLocal(ctx)
		case <-ctx.Done():
			log.Printf("status: %s service stopped", s.name)
			return
		}
	}
}

// serf tag keys carrying the domain payload. Name, Addr, Port, and Alive are
// recovered from the serf Member, so only role and MongoInfo travel as tags.
const (
	tagRole      = "role"
	tagLeader    = "leader"
	tagAPIPort   = "apiport"
	tagSet       = "set"
	tagMe        = "me"
	tagSharded   = "sharded"
	tagConfigSvr = "configsvr"
	tagPrimary   = "primary"
	tagSecondary = "secondary"
	tagHidden    = "hidden"
	tagPassive   = "passive"
	tagArbiter   = "arbiter"
)

// encodeTags renders role and MongoInfo as serf tags. Booleans are emitted only
// when true and strings only when non-empty, keeping the tag map small (serf caps
// the encoded set at 512 bytes) and the `serf members -tags` output readable.
func encodeTags(role Role, mi MongoInfo, isLeader bool, apiPort int) map[string]string {
	t := make(map[string]string)
	putStr(t, tagRole, string(role))
	putBool(t, tagLeader, isLeader)
	putInt(t, tagAPIPort, apiPort)
	putStr(t, tagSet, mi.SetName)
	putStr(t, tagMe, mi.Me)
	putBool(t, tagSharded, mi.Sharded)
	putBool(t, tagConfigSvr, mi.ConfigSvr)
	putBool(t, tagPrimary, mi.IsPrimary)
	putBool(t, tagSecondary, mi.Secondary)
	putBool(t, tagHidden, mi.Hidden)
	putBool(t, tagPassive, mi.Passive)
	putBool(t, tagArbiter, mi.ArbiterOnly)
	return t
}

// decodeTags is the inverse of encodeTags. A missing bool tag means false and a
// missing apiport means zero.
func decodeTags(t map[string]string) (role Role, isLeader bool, apiPort int, mi MongoInfo) {
	apiPort, _ = strconv.Atoi(t[tagAPIPort])
	return Role(t[tagRole]), t[tagLeader] != "", apiPort, MongoInfo{
		SetName:     t[tagSet],
		Me:          t[tagMe],
		Sharded:     t[tagSharded] != "",
		ConfigSvr:   t[tagConfigSvr] != "",
		IsPrimary:   t[tagPrimary] != "",
		Secondary:   t[tagSecondary] != "",
		Hidden:      t[tagHidden] != "",
		Passive:     t[tagPassive] != "",
		ArbiterOnly: t[tagArbiter] != "",
	}
}

func putStr(t map[string]string, k, v string) {
	if v != "" {
		t[k] = v
	}
}

func putBool(t map[string]string, k string, v bool) {
	if v {
		t[k] = "1"
	}
}

func putInt(t map[string]string, k string, v int) {
	if v > 0 {
		t[k] = strconv.Itoa(v)
	}
}

// apply merges a transport event into the cache.
func (s *Svc) apply(ev StatusEvent) {
	if ev.Name == s.name {
		s.applySelf(ev)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch ev.Kind {
	case MemberGone:
		delete(s.mems, ev.Name)
		return
	case MemberFailed:
		// Keep the last-known info but flag the agent unreachable.
		m := s.mems[ev.Name]
		m.Name, m.Addr, m.Port = ev.Name, ev.Addr, ev.Port
		m.Alive = false
		m.AgentStatus = SubsysStatus{Err: "agent unreachable (health check failed)"}
		s.mems[ev.Name] = m
		return
	}

	role, isLeader, apiPort, mi := decodeTags(ev.Payload)
	s.mems[ev.Name] = AgentInfo{
		Name:        ev.Name,
		Addr:        ev.Addr,
		Port:        ev.Port,
		APIPort:     apiPort,
		Role:        role,
		IsLeader:    isLeader,
		Alive:       ev.Alive,
		MongoInfo:   mi,
		AgentStatus: SubsysStatus{OK: true},
	}
}

// applySelf handles serf's echo of our own broadcasts.
// We need just update fields from the serf.
func (s *Svc) applySelf(ev StatusEvent) {
	if !ev.Alive {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	self := s.mems[s.name]
	self.Name, self.Addr, self.Port = ev.Name, ev.Addr, ev.Port
	s.mems[s.name] = self
}

// refreshLocal reads the local mongod, caches the local AgentInfo, and broadcasts
// it to the cluster.
func (s *Svc) refreshLocal(ctx context.Context) {
	mi, err := s.localMongoInfo(ctx)
	if err != nil {
		s.handleMongoErr(err)
		return
	}

	isLeader := s.IsLeader()

	s.mu.Lock()
	// Read-modify-write to preserve Addr/Port set by our own serf join event.
	self := s.mems[s.name]
	self.Name = s.name
	self.Role = s.role
	self.APIPort = s.apiPort
	self.IsLeader = isLeader
	self.Alive = true
	self.MongoInfo = mi
	self.AgentStatus = SubsysStatus{OK: true}
	s.mems[s.name] = self
	s.mu.Unlock()

	//todo as part of PBM-1748:
	//- change detection
	//- introduce agent's meta doc, instead of broadcasting every property
	//- improve error handling: db down, storage down, agent down

	if s.pub != nil {
		if err := s.pub.Publish(encodeTags(s.role, mi, isLeader, s.apiPort)); err != nil {
			log.Printf("status: publish local info: %v", err)
		}
	}
}

// localMongoInfo reads this agent's own mongod and maps its node info into MongoInfo.
// When the agent has no MongoDB attached, it is a no-op and returns the zero MongoInfo.
func (s *Svc) localMongoInfo(ctx context.Context) (MongoInfo, error) {
	if s.mc == nil {
		return MongoInfo{}, nil
	}

	// GetNodeInfoExt also reads the parsed command line, so IsSharded reflects the
	// node's clusterRole even before hello gossips $configServerState.
	ni, err := topo.GetNodeInfoExt(ctx, s.mc)
	if err != nil {
		return MongoInfo{}, fmt.Errorf("get local node info: %w", err)
	}

	return MongoInfo{
		SetName:     ni.SetName,
		Me:          ni.Me,
		Sharded:     ni.IsSharded(),
		ConfigSvr:   ni.IsConfigSrv(),
		IsPrimary:   ni.IsPrimary,
		Secondary:   ni.Secondary,
		Hidden:      ni.Hidden,
		Passive:     ni.Passive,
		ArbiterOnly: ni.ArbiterOnly,
	}, nil
}

func (s *Svc) handleMongoErr(mongoErr error) {
	log.Printf("status: read local mongo info: %v", mongoErr)

	s.mu.Lock()
	self := s.mems[s.name]
	self.MongoInfo.IsPrimary = false
	self.AgentStatus = SubsysStatus{Err: mongoErr.Error()}
	s.mems[s.name] = self
	s.mu.Unlock()

	if s.pub != nil {
		if err := s.pub.Publish(encodeTags(s.role, self.MongoInfo, false, s.apiPort)); err != nil {
			log.Printf("status: publish local info: %v", err)
		}
	}
}

// GetAllMembers returns all agents within PBM's cluster.
func (s *Svc) GetAllMembers() []AgentInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]AgentInfo, 0, len(s.mems))
	for _, m := range s.mems {
		out = append(out, m)
	}
	sortMembers(out)
	return out
}

// GetMembersForRS returns the agents belonging to the given replica set.
func (s *Svc) GetMembersForRS(rsName string) []AgentInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []AgentInfo
	for _, m := range s.mems {
		if m.MongoInfo.SetName == rsName {
			out = append(out, m)
		}
	}
	sortMembers(out)
	return out
}

// GetMember returns the cached member by node name, or ErrNotFound.
func (s *Svc) GetMember(node string) (*AgentInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	member, ok := s.mems[node]
	if !ok {
		return nil, ErrNotFound
	}
	return &member, nil
}

// Leader returns the cluster member currently advertising leadership,
// if one is known.
func (s *Svc) Leader() (AgentInfo, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, m := range s.mems {
		if m.IsLeader {
			return m, true
		}
	}
	return AgentInfo{}, false
}

// sortMembers orders members by node name ascending.
func sortMembers(m []AgentInfo) {
	sort.Slice(m, func(i, j int) bool {
		return m[i].Name < m[j].Name
	})
}
