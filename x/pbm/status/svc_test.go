package status

import (
	"errors"
	"testing"
)

func TestApplyCachesAndRemovesMembers(t *testing.T) {
	s := New("self", RoleWorker, nil, 0)

	s.apply(StatusEvent{
		Kind:    MemberUp,
		Name:    "rs0-0",
		Addr:    "10.0.0.1",
		Port:    7777,
		Alive:   true,
		Payload: encodeTags(RoleWorker, MongoInfo{SetName: "rs0", IsPrimary: true}, false, 0),
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
	s.apply(StatusEvent{
		Kind:    MemberUpdate,
		Name:    "rs0-0",
		Addr:    "10.0.0.1",
		Port:    7777,
		Alive:   true,
		Payload: encodeTags(RoleWorker, MongoInfo{SetName: "rs0", Secondary: true}, false, 0),
	})
	m, _ = s.GetMember("rs0-0")
	if m.MongoInfo.IsPrimary || !m.MongoInfo.Secondary {
		t.Fatalf("update not applied: %+v", m)
	}

	// Failed keeps the member but flags it unreachable, preserving last-known info.
	s.apply(StatusEvent{Kind: MemberFailed, Name: "rs0-0", Addr: "10.0.0.1", Port: 7777})
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

	// Gone removes it.
	s.apply(StatusEvent{Kind: MemberGone, Name: "rs0-0"})
	if _, err := s.GetMember("rs0-0"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after gone, got %v", err)
	}
}

func TestGetMembersForRSAndSorting(t *testing.T) {
	s := New("self", RoleWorker, nil, 0)
	s.apply(StatusEvent{Kind: MemberUp, Name: "rs1-0", Alive: true,
		Payload: encodeTags(RoleWorker, MongoInfo{SetName: "rs1"}, false, 0)})
	s.apply(StatusEvent{Kind: MemberUp, Name: "rs0-1", Alive: true,
		Payload: encodeTags(RoleWorker, MongoInfo{SetName: "rs0"}, false, 0)})
	s.apply(StatusEvent{Kind: MemberUp, Name: "rs0-0", Alive: true,
		Payload: encodeTags(RoleWorker, MongoInfo{SetName: "rs0"}, false, 0)})

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

// fakePublisher captures the last published tags.
type fakePublisher struct{ last map[string]string }

func (f *fakePublisher) Publish(tags map[string]string) error { f.last = tags; return nil }

// fakeLeadership is a static Leadership probe.
type fakeLeadership bool

func (f fakeLeadership) IsLeader() bool { return bool(f) }

func TestRefreshLocalCachesSelfAndPublishes(t *testing.T) {
	s := New("self", RoleCtrl, nil, 0)
	fp := &fakePublisher{}
	s.SetPublisher(fp)

	s.refreshLocal(t.Context())

	self, err := s.GetMember("self")
	if err != nil {
		t.Fatalf("self not cached: %v", err)
	}
	if self.Role != RoleCtrl || !self.Alive {
		t.Fatalf("unexpected self entry: %+v", self)
	}
	if fp.last == nil {
		t.Fatal("expected local info to be published")
	}
}

func TestLeadershipPropagates(t *testing.T) {
	// A ctrl agent that holds leadership caches and advertises the leader bit.
	s := New("self", RoleCtrl, nil, 0)
	fp := &fakePublisher{}
	s.SetPublisher(fp)
	s.SetLeaderChecker(fakeLeadership(true))

	s.refreshLocal(t.Context())

	self, _ := s.GetMember("self")
	if !self.IsLeader {
		t.Fatalf("ctrl leader should cache IsLeader=true: %+v", self)
	}
	if fp.last[tagLeader] == "" {
		t.Fatalf("leader bit should be advertised, got tags %v", fp.last)
	}

	// A peer decoding that broadcast sees the leader.
	peer := New("peer", RoleWorker, nil, 0)
	peer.apply(StatusEvent{Kind: MemberUp, Name: "self", Alive: true, Payload: fp.last})
	if m, _ := peer.GetMember("self"); !m.IsLeader {
		t.Fatalf("peer should observe leader: %+v", m)
	}
}

func TestSelfAdvertisesAPIPort(t *testing.T) {
	// A ctrl agent advertises its constructor-injected API port.
	s := New("self", RoleCtrl, nil, 9000)
	fp := &fakePublisher{}
	s.SetPublisher(fp)

	s.refreshLocal(t.Context())

	if self, _ := s.GetMember("self"); self.APIPort != 9000 {
		t.Fatalf("self should cache API port: %+v", self)
	}
	if fp.last[tagAPIPort] != "9000" {
		t.Fatalf("API port should be advertised, got tags %v", fp.last)
	}
}

func TestLeaderLookupAndAPIPort(t *testing.T) {
	s := New("self", RoleWorker, nil, 0)

	// A ctrl leader advertises its API port; the bit round-trips through tags.
	s.apply(StatusEvent{Kind: MemberUp, Name: "ctrl-0", Addr: "10.0.0.1", Alive: true,
		Payload: encodeTags(RoleCtrl, MongoInfo{}, true, 9000)})

	leader, ok := s.Leader()
	if !ok {
		t.Fatal("expected a known leader")
	}
	if leader.Name != "ctrl-0" || !leader.IsLeader || leader.APIPort != 9000 {
		t.Fatalf("unexpected leader: %+v", leader)
	}

	// With no advertised leader, Leader reports none.
	if _, ok := New("self", RoleWorker, nil, 0).Leader(); ok {
		t.Fatal("expected no leader when none advertised")
	}
}

func TestWorkerNeverLeader(t *testing.T) {
	// Workers carry no probe; leaving leadership unset must keep them followers.
	s := New("self", RoleWorker, nil, 0)
	fp := &fakePublisher{}
	s.SetPublisher(fp)

	s.refreshLocal(t.Context())

	if self, _ := s.GetMember("self"); self.IsLeader {
		t.Fatalf("worker must never be leader: %+v", self)
	}
	if fp.last[tagLeader] != "" {
		t.Fatalf("worker must not advertise leader bit, got tags %v", fp.last)
	}
}
