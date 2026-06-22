package status

import (
	"errors"
	"testing"
)

func TestApplyCachesAndRemovesMembers(t *testing.T) {
	s := New("self", RoleWorker, "")

	s.apply(StatusEvent{
		Kind:    MemberUp,
		Name:    "rs0-0",
		Addr:    "10.0.0.1",
		Port:    7777,
		Alive:   true,
		Payload: encodeTags(RoleWorker, MongoInfo{SetName: "rs0", IsPrimary: true}),
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
		Payload: encodeTags(RoleWorker, MongoInfo{SetName: "rs0", Secondary: true}),
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
	s := New("self", RoleWorker, "")
	s.apply(StatusEvent{Kind: MemberUp, Name: "rs1-0", Alive: true,
		Payload: encodeTags(RoleWorker, MongoInfo{SetName: "rs1"})})
	s.apply(StatusEvent{Kind: MemberUp, Name: "rs0-1", Alive: true,
		Payload: encodeTags(RoleWorker, MongoInfo{SetName: "rs0"})})
	s.apply(StatusEvent{Kind: MemberUp, Name: "rs0-0", Alive: true,
		Payload: encodeTags(RoleWorker, MongoInfo{SetName: "rs0"})})

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

func TestRefreshLocalCachesSelfAndPublishes(t *testing.T) {
	s := New("self", RoleCtrl, "")
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
