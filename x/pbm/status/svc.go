package status

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.uber.org/zap"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/topo"
)

const (
	// refreshInterval is how often an agent re-reads its local mongod for change monitoring purpose.
	refreshInterval = 5 * time.Second

	// ccDBTimeout bounds a single ccDB call attempt.
	ccDBTimeout = 2 * time.Second
	// ccDBAttempts is how many times a failed ccDB call is tried in total.
	ccDBAttempts = 3
	// ccDBRetryDelay is the pause between ccDB call attempts.
	ccDBRetryDelay = 500 * time.Millisecond
)

// tagRev is the only serf tag that agent publishes: the etcd revision of its agent doc.
const tagRev = "rev"

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

// agentDoc is the agent's info persisted in etcd.
// Name, Addr, Port, and Alive are owned by serf, so they aren't stored.
type agentDoc struct {
	Role        Role         `json:"role"`
	IsLeader    bool         `json:"isLeader"`
	APIPort     int          `json:"apiPort"`
	MongoInfo   MongoInfo    `json:"mongoInfo"`
	AgentStatus SubsysStatus `json:"agentStatus"`
}

// applyTo copies the doc's fields into m, leaving serf-owned fields intact.
func (d *agentDoc) applyTo(m *AgentInfo) {
	m.Role = d.Role
	m.IsLeader = d.IsLeader
	m.APIPort = d.APIPort
	m.MongoInfo = d.MongoInfo
	m.AgentStatus = d.AgentStatus
}

// Svc is the status service. It caches every cluster member's AgentInfo (built from
// StatusEvents received over the discovery layer). Periodically, it reads its own local mongod
// and broadcasts the local AgentInfo to all other peers.
type Svc struct {
	lg   *zap.Logger
	repo agentRepo
	// mc is a direct connection to this agent's local mongod.
	// It's nil when the agent has no attached.
	mc *mongo.Client
	// leader reports whether this agent is the cluster leader
	leader LeaderChecker
	pub    Publisher

	name string // this agent's serf node name
	role Role

	// apiPort is this agent's web API port, advertised to peers so a non-leader
	// can return the leader's API endpoint. Zero for agents with no API (workers).
	apiPort int

	recv ReceiveChannel

	// lastPub is the last agent doc successfully published, used for change detection
	lastPub *agentDoc

	mu   sync.RWMutex
	mems clusterMembers
}

// clusterMembers is the in-memory store of cluster members keyed by node name
type clusterMembers map[string]AgentInfo

// agentRepo persists agents' docs. It's implemented by Repo.
type agentRepo interface {
	Put(ctx context.Context, name string, doc agentDoc) (int64, error)
	Get(ctx context.Context, name string, rev int64) (agentDoc, error)
}

// ReceiveChannel carries StatusEvents from discovery
type ReceiveChannel chan StatusEvent

// LeaderChecker reports whether this agent currently holds cluster leadership.
type LeaderChecker interface {
	IsLeader() bool
}

// New creates a status service.
// apiPort is this agent's web API port, advertised to peers, pass 0 for agents
// with no API (workers).
func New(lg *zap.Logger, name string, role Role, localMongo *mongo.Client, apiPort int) *Svc {
	return &Svc{
		lg:      lg.Named("status"),
		name:    name,
		role:    role,
		mc:      localMongo,
		apiPort: apiPort,
		recv:    make(ReceiveChannel, 64), // todo: improve
		mems:    map[string]AgentInfo{},
	}
}

// NewForCtrlAgent creates a status service for ctrl-agent.
func NewForCtrlAgent(lg *zap.Logger, name string, localMongo *mongo.Client, apiPort int) *Svc {
	return New(lg, name, RoleCtrl, localMongo, apiPort)
}

// NewForWorkerAgent creates a status service for worker-agent.
func NewForWorkerAgent(lg *zap.Logger, name string, localMongo *mongo.Client) *Svc {
	return New(lg, name, RoleWorker, localMongo, 0)
}

// DiscoSync exposes the receive channel as a send-only end for the discovery layer.
func (s *Svc) DiscoSync() chan<- StatusEvent {
	return s.recv
}

// SetPublisher wires the broadcast using disco.
func (s *Svc) SetPublisher(p Publisher) {
	s.pub = p
}

// SetRepo wires the repository where agents' docs are stored.
func (s *Svc) SetRepo(r agentRepo) {
	s.repo = r
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
	// Separate goroutines, so a slow local mongod or etcd write doesn't block
	// draining of membership events
	var wg sync.WaitGroup
	wg.Go(func() { s.runEvents(ctx) })
	wg.Go(func() { s.runRefresh(ctx) })
	wg.Wait()

	s.lg.Info("service stopped")
}

// runEvents applies membership events.
func (s *Svc) runEvents(ctx context.Context) {
	for {
		select {
		case ev := <-s.recv:
			s.apply(ctx, ev)
		case <-ctx.Done():
			s.lg.Info("stoping applying agents related events")
			return
		}
	}
}

// runRefresh periodically refreshes and publishes local info.
func (s *Svc) runRefresh(ctx context.Context) {
	t := time.NewTicker(refreshInterval)
	defer t.Stop()

	s.refreshLocal(ctx)
	for {
		select {
		case <-t.C:
			s.refreshLocal(ctx)
		case <-ctx.Done():
			s.lg.Info("stoping refreshing mongodb info")
			return
		}
	}
}

// encodeRev renders the agent doc revision as serf tags.
func encodeRev(rev int64) map[string]string {
	return map[string]string{tagRev: strconv.FormatInt(rev, 10)}
}

// decodeRev is the inverse of encodeRev. A missing or malformed tag means zero.
func decodeRev(t map[string]string) int64 {
	rev, _ := strconv.ParseInt(t[tagRev], 10, 64)
	return rev
}

// apply merges a transport event into the cache.
func (s *Svc) apply(ctx context.Context, ev StatusEvent) {
	if ev.Name == s.name {
		s.applySelf(ev)
		return
	}

	switch ev.Kind {
	case MemberGone:
		s.mu.Lock()
		delete(s.mems, ev.Name)
		s.mu.Unlock()
		return
	case MemberFailed:
		// Keep the last-known info but flag the agent unreachable.
		s.mu.Lock()
		m := s.mems[ev.Name]
		m.Name, m.Addr, m.Port = ev.Name, ev.Addr, ev.Port
		m.Alive = false
		m.AgentStatus = SubsysStatus{Err: "agent unreachable (health check failed)"}
		s.mems[ev.Name] = m
		s.mu.Unlock()
		return
	}

	// MemberUp and MemberUpdate: refresh serf-owned fields and load the doc
	// for the advertised revision.
	m := AgentInfo{
		Name:        ev.Name,
		Addr:        ev.Addr,
		Port:        ev.Port,
		Alive:       ev.Alive,
		AgentStatus: SubsysStatus{Err: "agent info not loaded yet"},
	}
	s.mu.Lock()
	s.mems[ev.Name] = m
	s.mu.Unlock()

	rev := decodeRev(ev.Payload)
	if rev == 0 {
		return
	}
	s.loadPeer(ctx, ev.Name, rev)
}

// loadPeer fetches the peer's agent doc at the advertised revision from ccDB and applies it to the cached member.
func (s *Svc) loadPeer(ctx context.Context, name string, rev int64) {
	lg := s.lg.With(zap.String("member", name), zap.Int64("rev", rev))

	doc, err := s.getAgentDoc(ctx, lg, name, rev)
	if err != nil {
		lg.Warn("fetch agent info", zap.Error(err))
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.mems[name]
	doc.applyTo(&m)
	s.mems[name] = m
}

// putAgentDoc stores this agent's doc in the repo and returns its revision.
func (s *Svc) putAgentDoc(ctx context.Context, lg *zap.Logger, doc agentDoc) (int64, error) {
	var rev int64
	err := s.retry(ctx, lg, func(ctx context.Context) error {
		var err error
		rev, err = s.repo.Put(ctx, s.name, doc)
		return err
	})
	return rev, err
}

// getAgentDoc fetches the named agent's doc at the given revision from the repo.
func (s *Svc) getAgentDoc(ctx context.Context, lg *zap.Logger, name string, rev int64) (agentDoc, error) {
	var doc agentDoc
	err := s.retry(ctx, lg, func(ctx context.Context) error {
		var err error
		doc, err = s.repo.Get(ctx, name, rev)
		return err
	})
	return doc, err
}

func (s *Svc) retry(ctx context.Context, lg *zap.Logger, fn func(ctx context.Context) error) error {
	for attempt := 1; ; attempt++ {
		actx, cancel := context.WithTimeout(ctx, ccDBTimeout)
		err := fn(actx)
		cancel()
		if err == nil || errors.Is(err, ErrDocNotFound) || attempt == ccDBAttempts {
			return err
		}

		lg.Warn("etcd failed during storing agent info, retrying",
			zap.Int("attempt", attempt),
			zap.Int("max_attempts", ccDBAttempts),
			zap.Duration("retry_in", ccDBRetryDelay),
			zap.Error(err))

		select {
		case <-time.After(ccDBRetryDelay):
		case <-ctx.Done():
			return err
		}
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
		s.handleMongoErr(ctx, err)
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

	s.publish(ctx, agentDoc{
		Role:        s.role,
		IsLeader:    isLeader,
		APIPort:     s.apiPort,
		MongoInfo:   mi,
		AgentStatus: self.AgentStatus,
	})
}

// publish stores the agent doc in ccDB and advertises its revision over serf,
// unless the doc is identical to the last successfully published one.
func (s *Svc) publish(ctx context.Context, doc agentDoc) {
	if s.lastPub != nil && *s.lastPub == doc {
		// nothing has changed since last time
		return
	}

	rev, err := s.putAgentDoc(ctx, s.lg, doc)
	if err != nil {
		s.lg.Warn("store local info", zap.Error(err))
		return
	}
	if err := s.pub.Publish(encodeRev(rev)); err != nil {
		s.lg.Warn("publish local info", zap.Error(err))
		return
	}
	s.lastPub = &doc
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

func (s *Svc) handleMongoErr(ctx context.Context, mongoErr error) {
	s.lg.Warn("read local mongo info", zap.Error(mongoErr))

	s.mu.Lock()
	self := s.mems[s.name]
	self.MongoInfo.IsPrimary = false
	self.AgentStatus = SubsysStatus{Err: mongoErr.Error()}
	s.mems[s.name] = self
	s.mu.Unlock()

	s.publish(ctx, agentDoc{
		Role:        s.role,
		APIPort:     s.apiPort,
		MongoInfo:   self.MongoInfo,
		AgentStatus: self.AgentStatus,
	})
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
