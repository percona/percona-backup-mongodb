package status

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
)

func TestApplyCachesAndRemovesMembers(t *testing.T) {
	repo := &failingRepo{agentRepo: NewRepo(newEtcdClient(t))}
	s, _ := newTestSvc(t, "self", RoleWorker, 0, repo)

	rev := putDoc(t, repo, "rs0-0", agentDoc{
		Role:        RoleWorker,
		MongoInfo:   MongoInfo{SetName: "rs0", IsPrimary: true},
		AgentStatus: SubsysStatus{OK: true},
	})
	s.apply(t.Context(), StatusEvent{
		Kind:    MemberUp,
		Name:    "rs0-0",
		Addr:    "10.0.0.1",
		Port:    7777,
		Alive:   true,
		Payload: encodeRev(rev),
	})

	m, err := s.GetMember("rs0-0")
	if err != nil {
		t.Fatalf("GetMember after up: %v", err)
	}
	if !m.Alive || m.Role != RoleWorker || !m.MongoInfo.IsPrimary || m.MongoInfo.SetName != "rs0" {
		t.Fatalf("unexpected cached member: %+v", m)
	}
	if !m.AgentStatus.OK {
		t.Fatalf("alive member should have OK status: %+v", m.AgentStatus)
	}

	// Update flips primary -> secondary.
	rev = putDoc(t, repo, "rs0-0", agentDoc{
		Role:        RoleWorker,
		MongoInfo:   MongoInfo{SetName: "rs0", Secondary: true},
		AgentStatus: SubsysStatus{OK: true},
	})
	s.apply(t.Context(), StatusEvent{
		Kind:    MemberUpdate,
		Name:    "rs0-0",
		Addr:    "10.0.0.1",
		Port:    7777,
		Alive:   true,
		Payload: encodeRev(rev),
	})
	m, _ = s.GetMember("rs0-0")
	if m.MongoInfo.IsPrimary || !m.MongoInfo.Secondary {
		t.Fatalf("update not applied: %+v", m)
	}

	// Failed keeps the member but flags it unreachable, preserving last-known info.
	s.apply(t.Context(), StatusEvent{Kind: MemberFailed, Name: "rs0-0", Addr: "10.0.0.1", Port: 7777})
	m, err = s.GetMember("rs0-0")
	if err != nil {
		t.Fatalf("member should survive failure: %v", err)
	}
	if m.Alive || m.AgentStatus.OK || m.AgentStatus.Err == "" {
		t.Fatalf("failed member not flagged: %+v", m)
	}
	if !m.MongoInfo.Secondary {
		t.Fatalf("failure should preserve last-known mongo info: %+v", m.MongoInfo)
	}

	// Recovery with the same revision restores the status with a refetch.
	gets := repo.getCalls
	s.apply(t.Context(), StatusEvent{
		Kind: MemberUp, Name: "rs0-0", Addr: "10.0.0.1", Port: 7777, Alive: true,
		Payload: encodeRev(rev),
	})
	m, _ = s.GetMember("rs0-0")
	if !m.Alive || !m.AgentStatus.OK || !m.MongoInfo.Secondary {
		t.Fatalf("recovered member not restored: %+v", m)
	}
	if repo.getCalls != gets+1 {
		t.Fatalf("recovered member should be refetched once, gets %d -> %d", gets, repo.getCalls)
	}

	// Restart: the rejoin carries no rev yet, so the stale doc must not be shown.
	s.apply(t.Context(), StatusEvent{Kind: MemberFailed, Name: "rs0-0", Addr: "10.0.0.1", Port: 7777})
	s.apply(t.Context(), StatusEvent{Kind: MemberUp, Name: "rs0-0", Addr: "10.0.0.1", Port: 7777, Alive: true})
	m, _ = s.GetMember("rs0-0")
	if !m.Alive || m.AgentStatus.OK || m.MongoInfo.Secondary {
		t.Fatalf("rejoined member without rev should not expose stale info: %+v", m)
	}

	// Gone removes it.
	s.apply(t.Context(), StatusEvent{Kind: MemberGone, Name: "rs0-0"})
	if _, err := s.GetMember("rs0-0"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after gone, got %v", err)
	}
}

func TestGetMembersForRSAndSorting(t *testing.T) {
	repo := NewRepo(newEtcdClient(t))
	s, _ := newTestSvc(t, "self", RoleWorker, 0, repo)

	for _, m := range []struct{ name, rs string }{
		{"rs1-0", "rs1"},
		{"rs0-1", "rs0"},
		{"rs0-0", "rs0"},
	} {
		rev := putDoc(t, repo, m.name, agentDoc{Role: RoleWorker, MongoInfo: MongoInfo{SetName: m.rs}})
		s.apply(t.Context(), StatusEvent{Kind: MemberUp, Name: m.name, Alive: true, Payload: encodeRev(rev)})
	}

	rs0 := s.GetMembersForRS("rs0")
	if len(rs0) != 2 {
		t.Fatalf("expected 2 members in rs0, got %d", len(rs0))
	}

	all := s.GetAllMembers()
	if len(all) != 3 {
		t.Fatalf("expected 3 members, got %d", len(all))
	}
	want := []string{"rs0-0", "rs0-1", "rs1-0"} // sorted by (setName, name)
	for i, w := range want {
		if all[i].Name != w {
			t.Fatalf("sort order: got %s at %d, want %s", all[i].Name, i, w)
		}
	}
}

func TestRefreshLocalStoresAndPublishesRev(t *testing.T) {
	cli := newEtcdClient(t)
	s, fp := newTestSvc(t, "self", RoleCtrl, 0, NewRepo(cli))

	s.refreshLocal(t.Context())

	self, err := s.GetMember("self")
	if err != nil {
		t.Fatalf("self not cached: %v", err)
	}
	if self.Role != RoleCtrl || !self.Alive {
		t.Fatalf("unexpected self entry: %+v", self)
	}
	doc, rev := storedDoc(t, cli, "self")
	if doc.Role != RoleCtrl || !doc.AgentStatus.OK {
		t.Fatalf("unexpected stored doc: %+v", doc)
	}
	if got := decodeRev(fp.last); got != rev {
		t.Fatalf("published rev %d, want stored doc rev %d", got, rev)
	}
}

func TestPublishOnlyOnChange(t *testing.T) {
	cli := newEtcdClient(t)
	s, fp := newTestSvc(t, "self", RoleCtrl, 0, NewRepo(cli))

	s.refreshLocal(t.Context())
	_, rev1 := storedDoc(t, cli, "self")
	s.refreshLocal(t.Context())
	if _, rev := storedDoc(t, cli, "self"); rev != rev1 || fp.calls != 1 {
		t.Fatalf("unchanged info should be stored and published once, rev %d -> %d, publishes %d",
			rev1, rev, fp.calls)
	}

	s.SetLeaderChecker(fakeLeadership(true))
	s.refreshLocal(t.Context())
	_, rev2 := storedDoc(t, cli, "self")
	if rev2 <= rev1 || fp.calls != 2 {
		t.Fatalf("changed info should be stored and published again, rev %d -> %d, publishes %d",
			rev1, rev2, fp.calls)
	}
	if decodeRev(fp.last) != rev2 {
		t.Fatalf("expected the new revision %d to be published, got tags %v", rev2, fp.last)
	}
}

func TestPublishRetriedAfterFailure(t *testing.T) {
	cli := newEtcdClient(t)
	repo := &failingRepo{agentRepo: NewRepo(cli), putFails: ccDBAttempts}
	s, fp := newTestSvc(t, "self", RoleCtrl, 0, repo)

	// store failure: nothing is published
	s.refreshLocal(t.Context())
	if fp.calls != 0 {
		t.Fatalf("nothing should be published when the store fails, got %d", fp.calls)
	}

	// publish failure: the doc is stored, the rev is not advertised
	fp.err = errors.New("serf down")
	s.refreshLocal(t.Context())
	_, rev1 := storedDoc(t, cli, "self")
	if fp.calls != 0 {
		t.Fatalf("expected the doc to be stored but not published, publishes %d", fp.calls)
	}
	fp.err = nil

	// the unpublished doc is stored again and its new rev gets advertised
	s.refreshLocal(t.Context())
	_, rev2 := storedDoc(t, cli, "self")
	if rev2 <= rev1 || fp.calls != 1 {
		t.Fatalf("expected store and publish retry, rev %d -> %d, publishes %d", rev1, rev2, fp.calls)
	}
	if got := decodeRev(fp.last); got != rev2 {
		t.Fatalf("published rev %d, want latest stored rev %d", got, rev2)
	}

	// once published, the unchanged doc is neither stored nor published again
	s.refreshLocal(t.Context())
	if _, rev := storedDoc(t, cli, "self"); rev != rev2 || fp.calls != 1 {
		t.Fatalf("unchanged doc should be skipped, rev %d -> %d, publishes %d", rev2, rev, fp.calls)
	}
}

func TestMongoErrPropagates(t *testing.T) {
	cli := newEtcdClient(t)
	s, _ := newTestSvc(t, "self", RoleWorker, 0, NewRepo(cli))

	s.handleMongoErr(t.Context(), errors.New("mongo down"))

	if doc, _ := storedDoc(t, cli, "self"); doc.AgentStatus.OK || doc.AgentStatus.Err != "mongo down" {
		t.Fatalf("mongo error should be stored: %+v", doc.AgentStatus)
	}
}

func TestPeerFetchFailure(t *testing.T) {
	repo := &failingRepo{agentRepo: NewRepo(newEtcdClient(t)), getFails: ccDBAttempts}
	s, _ := newTestSvc(t, "self", RoleWorker, 0, repo)
	rev := putDoc(t, repo, "rs0-0", agentDoc{Role: RoleWorker, AgentStatus: SubsysStatus{OK: true}})

	s.apply(t.Context(), StatusEvent{Kind: MemberUp, Name: "rs0-0", Alive: true, Payload: encodeRev(rev)})
	m, _ := s.GetMember("rs0-0")
	if !m.Alive || m.AgentStatus.OK || m.Role != "" {
		t.Fatalf("member with unloaded info should be alive but not OK: %+v", m)
	}

	// The peer is loaded on its next update.
	rev = putDoc(t, repo, "rs0-0", agentDoc{Role: RoleWorker, IsLeader: true, AgentStatus: SubsysStatus{OK: true}})
	s.apply(t.Context(), StatusEvent{Kind: MemberUpdate, Name: "rs0-0", Alive: true, Payload: encodeRev(rev)})
	m, _ = s.GetMember("rs0-0")
	if m.Role != RoleWorker || !m.IsLeader || !m.AgentStatus.OK {
		t.Fatalf("next update not applied: %+v", m)
	}
}

func TestPeerLoadsAdvertisedRev(t *testing.T) {
	repo := NewRepo(newEtcdClient(t))
	s, _ := newTestSvc(t, "self", RoleWorker, 0, repo)
	rev1 := putDoc(t, repo, "rs0-0", agentDoc{Role: RoleWorker, AgentStatus: SubsysStatus{OK: true}})
	rev2 := putDoc(t, repo, "rs0-0", agentDoc{Role: RoleWorker, IsLeader: true, AgentStatus: SubsysStatus{OK: true}})

	// The older update is applied as advertised, even though a newer doc exists.
	s.apply(t.Context(), StatusEvent{Kind: MemberUp, Name: "rs0-0", Alive: true, Payload: encodeRev(rev1)})
	if m, _ := s.GetMember("rs0-0"); m.IsLeader || !m.AgentStatus.OK {
		t.Fatalf("doc at rev %d should be applied: %+v", rev1, m)
	}

	// The newer one comes with its own update.
	s.apply(t.Context(), StatusEvent{Kind: MemberUpdate, Name: "rs0-0", Alive: true, Payload: encodeRev(rev2)})
	if m, _ := s.GetMember("rs0-0"); !m.IsLeader {
		t.Fatalf("doc at rev %d should be applied: %+v", rev2, m)
	}
}

func TestPeerRevWithoutDoc(t *testing.T) {
	repo := NewRepo(newEtcdClient(t))
	s, _ := newTestSvc(t, "self", RoleWorker, 0, repo)

	// rs0-0 advertises a revision at which it has no doc
	s.apply(t.Context(), StatusEvent{
		Kind: MemberUp, Name: "rs0-0", Alive: true,
		Payload: encodeRev(unusedRev(t, repo, "rs0-0")),
	})
	m, _ := s.GetMember("rs0-0")
	if !m.Alive || m.AgentStatus.OK || m.Role != "" {
		t.Fatalf("member without doc should be alive but not OK: %+v", m)
	}

	// Once the doc shows up, it's loaded on the next update.
	rev := putDoc(t, repo, "rs0-0", agentDoc{Role: RoleWorker, AgentStatus: SubsysStatus{OK: true}})
	s.apply(t.Context(), StatusEvent{Kind: MemberUpdate, Name: "rs0-0", Alive: true, Payload: encodeRev(rev)})
	if m, _ := s.GetMember("rs0-0"); m.Role != RoleWorker || !m.AgentStatus.OK {
		t.Fatalf("doc stored later should be loaded: %+v", m)
	}
}

func TestRunRefreshDoesNotBlockEvents(t *testing.T) {
	repo := NewRepo(newEtcdClient(t))
	s, _ := newTestSvc(t, "self", RoleWorker, 0, repo)
	bl := &blockingLeadership{entered: make(chan struct{}), release: make(chan struct{})}
	s.SetLeaderChecker(bl)
	rev := putDoc(t, repo, "rs0-0", agentDoc{Role: RoleWorker, AgentStatus: SubsysStatus{OK: true}})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	// the refresh goroutine is stuck reading local info
	<-bl.entered

	s.DiscoSync() <- StatusEvent{Kind: MemberUp, Name: "rs0-0", Alive: true, Payload: encodeRev(rev)}
	deadline := time.After(5 * time.Second)
	for {
		if m, err := s.GetMember("rs0-0"); err == nil && m.AgentStatus.OK {
			break
		}
		select {
		case <-deadline:
			t.Fatal("event not applied while refresh is blocked")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	close(bl.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}

func TestCCDBRetry(t *testing.T) {
	// One keyspace for the parallel subtests; each uses its own agent names.
	cli := newEtcdClient(t)

	// newSvc returns a svc whose warnings are recorded.
	newSvc := func(name string, repo agentRepo) (*Svc, *fakePublisher, *observer.ObservedLogs) {
		core, logs := observer.New(zap.WarnLevel)
		s, fp := newTestSvcWithLogger(zap.New(core), name, RoleWorker, 0, repo)
		return s, fp, logs
	}
	retryLogs := func(logs *observer.ObservedLogs) int {
		return logs.FilterMessage(retryMsg).Len()
	}

	t.Run("store is retried and logged", func(t *testing.T) {
		t.Parallel()

		repo := &failingRepo{agentRepo: NewRepo(cli), putFails: 2}
		s, fp, logs := newSvc("store-retried", repo)

		s.refreshLocal(t.Context())

		if repo.putCalls != 3 || fp.calls != 1 {
			t.Fatalf("store should succeed on 3rd attempt and be published, calls %d publishes %d",
				repo.putCalls, fp.calls)
		}
		if _, rev := storedDoc(t, cli, "store-retried"); decodeRev(fp.last) != rev {
			t.Fatalf("published rev %d, want stored rev %d", decodeRev(fp.last), rev)
		}
		if n := retryLogs(logs); n != 2 {
			t.Fatalf("expected 2 retry warnings, got %d", n)
		}
	})

	t.Run("fetch is retried and logged", func(t *testing.T) {
		t.Parallel()

		repo := &failingRepo{agentRepo: NewRepo(cli)}
		s, _, logs := newSvc("fetch-retried", repo)
		rev := putDoc(t, repo, "fetch-peer", agentDoc{Role: RoleWorker, AgentStatus: SubsysStatus{OK: true}})
		repo.getFails = 2

		s.apply(t.Context(), StatusEvent{Kind: MemberUp, Name: "fetch-peer", Alive: true, Payload: encodeRev(rev)})

		if m, _ := s.GetMember("fetch-peer"); m.Role != RoleWorker || !m.AgentStatus.OK {
			t.Fatalf("fetch should succeed on 3rd attempt: %+v", m)
		}
		if repo.getCalls != 3 {
			t.Fatalf("expected 3 get calls, got %d", repo.getCalls)
		}
		retries := logs.FilterMessage(retryMsg).FilterField(zap.String("member", "fetch-peer"))
		if retries.Len() != 2 {
			t.Fatalf("expected 2 retry warnings for the member, got %d", retries.Len())
		}
	})

	t.Run("gives up after max attempts", func(t *testing.T) {
		t.Parallel()

		repo := &failingRepo{agentRepo: NewRepo(cli), putFails: ccDBAttempts}
		s, fp, logs := newSvc("gives-up", repo)

		s.refreshLocal(t.Context())

		if repo.putCalls != ccDBAttempts || fp.calls != 0 {
			t.Fatalf("expected %d attempts and no publish, calls %d publishes %d",
				ccDBAttempts, repo.putCalls, fp.calls)
		}
		if n := retryLogs(logs); n != ccDBAttempts-1 {
			t.Fatalf("expected %d retry warnings, got %d", ccDBAttempts-1, n)
		}
		if logs.FilterMessage("store local info").Len() != 1 {
			t.Fatal("expected the final failure to be logged")
		}
	})

	t.Run("missing doc is not retried", func(t *testing.T) {
		t.Parallel()

		repo := &failingRepo{agentRepo: NewRepo(cli)}
		s, _, logs := newSvc("missing", repo)
		rev := unusedRev(t, repo, "missing-peer")

		s.apply(t.Context(), StatusEvent{Kind: MemberUp, Name: "missing-peer", Alive: true, Payload: encodeRev(rev)})

		if repo.getCalls != 1 {
			t.Fatalf("expected a single get call, got %d", repo.getCalls)
		}
		if n := retryLogs(logs); n != 0 {
			t.Fatalf("expected no retry warnings, got %d", n)
		}
	})
}

func TestLeadershipPropagates(t *testing.T) {
	// A ctrl agent that holds leadership caches and advertises the leader bit.
	cli := newEtcdClient(t)
	repo := NewRepo(cli)
	s, fp := newTestSvc(t, "self", RoleCtrl, 0, repo)
	s.SetLeaderChecker(fakeLeadership(true))

	s.refreshLocal(t.Context())

	self, _ := s.GetMember("self")
	if !self.IsLeader {
		t.Fatalf("ctrl leader should cache IsLeader=true: %+v", self)
	}
	if doc, _ := storedDoc(t, cli, "self"); !doc.IsLeader {
		t.Fatalf("leader bit should be stored, got doc %+v", doc)
	}

	// A peer receiving that broadcast sees the leader.
	peer, _ := newTestSvc(t, "peer", RoleWorker, 0, repo)
	peer.apply(t.Context(), StatusEvent{Kind: MemberUp, Name: "self", Alive: true, Payload: fp.last})
	if m, _ := peer.GetMember("self"); !m.IsLeader {
		t.Fatalf("peer should observe leader: %+v", m)
	}
}

func TestSelfAdvertisesAPIPort(t *testing.T) {
	// A ctrl agent advertises its constructor-injected API port.
	cli := newEtcdClient(t)
	s, _ := newTestSvc(t, "self", RoleCtrl, 9000, NewRepo(cli))

	s.refreshLocal(t.Context())

	if self, _ := s.GetMember("self"); self.APIPort != 9000 {
		t.Fatalf("self should cache API port: %+v", self)
	}
	if doc, _ := storedDoc(t, cli, "self"); doc.APIPort != 9000 {
		t.Fatalf("API port should be stored, got doc %+v", doc)
	}
}

func TestLeaderLookupAndAPIPort(t *testing.T) {
	repo := NewRepo(newEtcdClient(t))
	s, _ := newTestSvc(t, "self", RoleWorker, 0, repo)

	// A ctrl leader advertises its API port; the bit round-trips through ccDB.
	rev := putDoc(t, repo, "ctrl-0", agentDoc{Role: RoleCtrl, IsLeader: true, APIPort: 9000})
	s.apply(t.Context(), StatusEvent{
		Kind: MemberUp, Name: "ctrl-0", Addr: "10.0.0.1", Alive: true,
		Payload: encodeRev(rev),
	})

	leader, ok := s.Leader()
	if !ok {
		t.Fatal("expected a known leader")
	}
	if leader.Name != "ctrl-0" || !leader.IsLeader || leader.APIPort != 9000 {
		t.Fatalf("unexpected leader: %+v", leader)
	}

	// With no advertised leader, Leader reports none.
	if _, ok := New(zaptest.NewLogger(t), "self", RoleWorker, nil, 0).Leader(); ok {
		t.Fatal("expected no leader when none advertised")
	}
}

func TestWorkerNeverLeader(t *testing.T) {
	// Workers carry no probe; leaving leadership unset must keep them followers.
	cli := newEtcdClient(t)
	s, _ := newTestSvc(t, "self", RoleWorker, 0, NewRepo(cli))

	s.refreshLocal(t.Context())

	if self, _ := s.GetMember("self"); self.IsLeader {
		t.Fatalf("worker must never be leader: %+v", self)
	}
	if doc, _ := storedDoc(t, cli, "self"); doc.IsLeader {
		t.Fatalf("worker must not advertise leader bit, got doc %+v", doc)
	}
}

const retryMsg = "etcd failed during storing agent info, retrying"

var errDown = errors.New("etcd down")

// failingRepo fails the first putFails Put and getFails Get calls with errDown,
// then delegates to the embedded repo. It counts all calls.
type failingRepo struct {
	agentRepo

	putFails int
	getFails int
	putCalls int
	getCalls int
}

func (f *failingRepo) Put(ctx context.Context, name string, doc agentDoc) (int64, error) {
	f.putCalls++
	if f.putCalls <= f.putFails {
		return 0, errDown
	}
	return f.agentRepo.Put(ctx, name, doc)
}

func (f *failingRepo) Get(ctx context.Context, name string, rev int64) (agentDoc, error) {
	f.getCalls++
	if f.getCalls <= f.getFails {
		return agentDoc{}, errDown
	}
	return f.agentRepo.Get(ctx, name, rev)
}

// putDoc stores an agent doc as the named agent would and returns its revision.
func putDoc(t *testing.T, repo agentRepo, name string, doc agentDoc) int64 {
	t.Helper()

	rev, err := repo.Put(t.Context(), name, doc)
	if err != nil {
		t.Fatalf("put doc: %v", err)
	}
	return rev
}

// storedDoc reads the named agent's latest doc from etcd with its revision.
func storedDoc(t *testing.T, cli *clientv3.Client, name string) (agentDoc, int64) {
	t.Helper()

	resp, err := cli.Get(t.Context(), key(name))
	if err != nil {
		t.Fatalf("get doc: %v", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatalf("no doc stored for %s", name)
	}
	var doc agentDoc
	if err := json.Unmarshal(resp.Kvs[0].Value, &doc); err != nil {
		t.Fatalf("unmarshal doc: %v", err)
	}
	return doc, resp.Kvs[0].ModRevision
}

// unusedRev returns an existing etcd revision at which the named agent has no
// doc, by storing a doc of another agent.
func unusedRev(t *testing.T, repo agentRepo, name string) int64 {
	t.Helper()

	return putDoc(t, repo, name+"-other", agentDoc{Role: RoleWorker})
}

func newTestSvc(t *testing.T, name string, role Role, apiPort int, repo agentRepo) (*Svc, *fakePublisher) {
	t.Helper()

	return newTestSvcWithLogger(zaptest.NewLogger(t), name, role, apiPort, repo)
}

func newTestSvcWithLogger(lg *zap.Logger, name string, role Role, apiPort int, repo agentRepo) (*Svc, *fakePublisher) {
	s := New(lg, name, role, nil, apiPort)
	fp := &fakePublisher{}
	s.SetPublisher(fp)
	s.SetRepo(repo)
	return s, fp
}

// fakePublisher captures the last published tags.
type fakePublisher struct {
	last  map[string]string
	calls int
	err   error
}

func (f *fakePublisher) Publish(tags map[string]string) error {
	if f.err != nil {
		return f.err
	}
	f.calls++
	f.last = tags
	return nil
}

// fakeLeadership is a static Leadership probe.
type fakeLeadership bool

func (f fakeLeadership) IsLeader() bool { return bool(f) }

// blockingLeadership blocks IsLeader until release is closed.
type blockingLeadership struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingLeadership) IsLeader() bool {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return false
}
