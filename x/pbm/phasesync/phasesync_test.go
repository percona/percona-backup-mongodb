package phasesync

import (
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	tcetcd "github.com/testcontainers/testcontainers-go/modules/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
)

const etcdImage = "gcr.io/etcd-development/etcd:v3.6.12"

var backupPhases = []string{"preparing", "starting", "running", "done"}

const waitTimeout = 500 * time.Millisecond

var (
	testEndpoints []string
	ctlCli        *clientv3.Client
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	ctr, err := tcetcd.Run(ctx, etcdImage)
	if err != nil {
		log.Fatalf("start etcd container: %v", err)
	}

	testEndpoints, err = ctr.ClientEndpoints(ctx)
	if err != nil {
		_ = ctr.Terminate(ctx)
		log.Fatalf("get etcd client endpoints: %v", err)
	}

	ctlCli, err = clientv3.New(clientv3.Config{
		Endpoints:   testEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		_ = ctr.Terminate(ctx)
		log.Fatalf("dial control client: %v", err)
	}

	code := m.Run()

	_ = ctlCli.Close()

	if err := ctr.Terminate(ctx); err != nil {
		log.Printf("terminate etcd container: %v", err)
	}

	os.Exit(code)
}

func TestNew(t *testing.T) {
	valid := Options{Prefix: "/test/TestNew/", ID: "rs0", Size: 1, Phases: backupPhases}

	t.Run("rejects incomplete options", func(t *testing.T) {
		for name, opts := range map[string]Options{
			"no prefix": {ID: "rs0", Size: 1, Phases: backupPhases},
			"no id":     {Prefix: valid.Prefix, Size: 1, Phases: backupPhases},
			"bad size":  {Prefix: valid.Prefix, ID: "rs0", Size: -1, Phases: backupPhases},
			"no phases": {Prefix: valid.Prefix, ID: "rs0", Size: 1},
		} {
			t.Run(name, func(t *testing.T) {
				b, err := New(t.Context(), newEtcdClient(t), opts)
				if err == nil {
					_ = b.Close()
					t.Fatal("New: got nil, want error")
				}
			})
		}
	})

	t.Run("size defaults to a group of one", func(t *testing.T) {
		prefix := testPrefix(t)
		b, err := New(t.Context(), newEtcdClient(t), Options{
			Prefix: prefix,
			ID:     "rs0",
			Phases: backupPhases,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })

		if err := b.Advance(t.Context(), "preparing"); err != nil {
			t.Fatalf("Advance: %v", err)
		}
	})

	t.Run("prefix without a trailing slash still matches", func(t *testing.T) {
		prefix := testPrefix(t)
		joinMember(t, prefix, "rs1", "running")

		b, err := New(t.Context(), newEtcdClient(t), Options{
			Prefix: strings.TrimSuffix(prefix, "/"),
			ID:     "rs0",
			Size:   2,
			Phases: backupPhases,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })

		if err := b.Advance(t.Context(), "starting"); err != nil {
			t.Fatalf("Advance: %v", err)
		}
	})

	t.Run("sub-second ttl falls back to the default", func(t *testing.T) {
		b, err := New(t.Context(), newEtcdClient(t), Options{
			Prefix: testPrefix(t),
			ID:     "rs0",
			Phases: backupPhases,
			TTL:    500 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })

		resp, err := ctlCli.TimeToLive(t.Context(), b.sess.Lease())
		if err != nil {
			t.Fatalf("TimeToLive: %v", err)
		}

		want := int64(DefaultTTL.Seconds())
		if resp.GrantedTTL != want {
			t.Errorf("granted TTL: got %ds, want %ds", resp.GrantedTTL, want)
		}
	})

	t.Run("rejects nil client", func(t *testing.T) {
		if _, err := New(t.Context(), nil, valid); err == nil {
			t.Fatal("New: got nil, want error")
		}
	})

	t.Run("announces the member as alive before any phase", func(t *testing.T) {
		prefix := testPrefix(t)
		b := newBarrier(t, prefix, "rs0", 2)

		// The member is visible, with no phase announced yet, and its key is
		// held by the lease so a crash would take it away.
		phase, found, leased := memberKey(t, prefix, "rs0")
		if !found {
			t.Fatal("member key: not found, want one")
		}
		if phase != "" {
			t.Errorf("phase: got %q, want empty", phase)
		}
		if !leased {
			t.Error("member key is not held by a lease")
		}

		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
}

func TestAdvance(t *testing.T) {
	t.Run("lone agent clears the barrier immediately", func(t *testing.T) {
		prefix := testPrefix(t)
		b := newTestBarrier(t, prefix, "rs0", 1)

		for _, phase := range backupPhases {
			if err := b.Advance(t.Context(), phase); err != nil {
				t.Fatalf("Advance(%q): %v", phase, err)
			}
		}
	})

	t.Run("group walks the backup phases in lockstep", func(t *testing.T) {
		const size = 4

		prefix := testPrefix(t)
		// arrived[i] counts the agents that announced phase i; an agent released
		// from that barrier must see all of them.
		arrived := make([]atomic.Int64, len(backupPhases))

		errc := make(chan error, size)
		var wg sync.WaitGroup
		for _, id := range []string{"rs0", "rs1", "rs2", "cfg"} {
			wg.Go(func() {
				b := newTestBarrier(t, prefix, id, size)
				for i, phase := range backupPhases {
					arrived[i].Add(1)
					if err := b.Advance(t.Context(), phase); err != nil {
						errc <- err
						return
					}
					if n := arrived[i].Load(); n != size {
						t.Errorf("%s released from %q with %d/%d agents arrived",
							id, phase, n, size)
					}
				}
			})
		}

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()

		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("agents did not finish all phases")
		}

		close(errc)
		for err := range errc {
			t.Errorf("Advance: %v", err)
		}
	})

	t.Run("blocks until the last agent arrives", func(t *testing.T) {
		prefix := testPrefix(t)
		fast := newTestBarrier(t, prefix, "rs0", 2)
		slow := newTestBarrier(t, prefix, "rs1", 2)

		released := advanceAsync(t, fast, "starting")

		select {
		case err := <-released:
			t.Fatalf("Advance returned before the group was complete: %v", err)
		case <-time.After(waitTimeout):
		}

		if err := slow.Advance(t.Context(), "starting"); err != nil {
			t.Fatalf("Advance(slow): %v", err)
		}

		mustRelease(t, released, "Advance(fast)")
	})

	t.Run("agent ahead of the barrier still counts", func(t *testing.T) {
		prefix := testPrefix(t)
		// rs1 raced ahead to "running" while rs0 is only now announcing
		// "starting"; being past the barrier satisfies it.
		joinMember(t, prefix, "rs1", "running")

		b := newTestBarrier(t, prefix, "rs0", 2)
		if err := b.Advance(t.Context(), "starting"); err != nil {
			t.Fatalf("Advance: %v", err)
		}
	})

	t.Run("agent that arrives while we watch releases the barrier", func(t *testing.T) {
		prefix := testPrefix(t)
		lease := joinMember(t, prefix, "rs1", "preparing")

		b := newTestBarrier(t, prefix, "rs0", 2)
		released := advanceAsync(t, b, "running")

		select {
		case err := <-released:
			t.Fatalf("Advance returned while rs1 was still preparing: %v", err)
		case <-time.After(waitTimeout):
		}

		announce(t, prefix, "rs1", "running", lease)

		mustRelease(t, released, "Advance")
	})

	t.Run("phase already reached is rejected", func(t *testing.T) {
		b := newTestBarrier(t, testPrefix(t), "rs0", 1)

		if err := b.Advance(t.Context(), "running"); err != nil {
			t.Fatalf("Advance: %v", err)
		}
		if err := b.Advance(t.Context(), "running"); err == nil {
			t.Error("Advance to the same phase: got nil, want error")
		}
		if err := b.Advance(t.Context(), "starting"); err == nil {
			t.Error("Advance to an earlier phase: got nil, want error")
		}
	})

	t.Run("unknown phase is rejected", func(t *testing.T) {
		b := newTestBarrier(t, testPrefix(t), "rs0", 1)

		if err := b.Advance(t.Context(), "finalizing"); err == nil {
			t.Fatal("Advance with unknown phase: got nil, want error")
		}
	})

	t.Run("canceled context unblocks the barrier", func(t *testing.T) {
		prefix := testPrefix(t)
		b := newTestBarrier(t, prefix, "rs0", 2)

		ctx, cancel := context.WithCancel(t.Context())
		released := make(chan error, 1)
		go func() { released <- b.Advance(ctx, "starting") }()

		select {
		case err := <-released:
			t.Fatalf("Advance returned before cancel: %v", err)
		case <-time.After(waitTimeout):
		}

		cancel()

		select {
		case err := <-released:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Advance: got %v, want context.Canceled", err)
			}
		case <-time.After(waitTimeout):
			t.Fatal("Advance did not return after cancel")
		}
	})
}

func TestAdvanceMemberLost(t *testing.T) {
	t.Run("member dies while we watch", func(t *testing.T) {
		prefix := testPrefix(t)
		// rs1 announced "preparing" and then died: the group must not wait on
		// it for the rest of the backup.
		lease := joinMember(t, prefix, "rs1", "preparing")

		b := newTestBarrier(t, prefix, "rs0", 2)
		released := advanceAsync(t, b, "running")

		select {
		case err := <-released:
			t.Fatalf("Advance returned while rs1 was still alive: %v", err)
		case <-time.After(waitTimeout):
		}

		revoke(t, lease)

		select {
		case err := <-released:
			if !errors.Is(err, ErrMemberLost) {
				t.Fatalf("Advance: got %v, want ErrMemberLost", err)
			}
		case <-time.After(waitTimeout):
			t.Fatal("Advance did not report the lost member")
		}
	})

	// A member gone before we looked leaves no trace, so it cannot be told
	// apart from one that has not started yet: the group waits, and the
	// caller's deadline is the backstop.
	t.Run("member died before we looked is waited for", func(t *testing.T) {
		prefix := testPrefix(t)
		lease := joinMember(t, prefix, "rs1", "preparing")
		revoke(t, lease)

		b := newTestBarrier(t, prefix, "rs0", 2)
		released := advanceAsync(t, b, "running")

		select {
		case err := <-released:
			t.Fatalf("Advance returned for a member gone before we looked: %v", err)
		case <-time.After(waitTimeout):
		}
	})

	// Clearing a barrier is no license to leave: the member is still needed for
	// the phases after it, and its key is gone for good.
	t.Run("member dies after clearing this barrier", func(t *testing.T) {
		prefix := testPrefix(t)
		lease := joinMember(t, prefix, "rs1", "preparing")
		joinMember(t, prefix, "rs2", "preparing")

		b := newTestBarrier(t, prefix, "rs0", 3)
		released := advanceAsync(t, b, "running")

		select {
		case err := <-released:
			t.Fatalf("Advance returned while the group was short: %v", err)
		case <-time.After(waitTimeout):
		}

		announce(t, prefix, "rs1", "running", lease)
		revoke(t, lease)

		select {
		case err := <-released:
			if !errors.Is(err, ErrMemberLost) {
				t.Fatalf("Advance: got %v, want ErrMemberLost", err)
			}
		case <-time.After(waitTimeout):
			t.Fatal("Advance did not report the lost member")
		}
	})

	t.Run("member that never started is waited for", func(t *testing.T) {
		prefix := testPrefix(t)
		// No trace of rs1 at all: it may still be booting, so the group waits
		// for it rather than declaring it lost.
		b := newTestBarrier(t, prefix, "rs0", 2)
		released := advanceAsync(t, b, "preparing")

		select {
		case err := <-released:
			t.Fatalf("Advance returned for a member that never started: %v", err)
		case <-time.After(waitTimeout):
		}

		joinMember(t, prefix, "rs1", "preparing")

		mustRelease(t, released, "Advance")
	})
}

func TestAdvanceReplicaSet(t *testing.T) {
	t.Run("clears every barrier without waiting", func(t *testing.T) {
		prefix := testPrefix(t)
		b := newTestBarrier(t, prefix, "rs0", 1)

		for _, phase := range backupPhases {
			released := advanceAsync(t, b, phase)

			select {
			case err := <-released:
				if err != nil {
					t.Fatalf("Advance(%q): %v", phase, err)
				}
			case <-time.After(waitTimeout):
				t.Fatalf("Advance(%q): blocked with nobody to wait for", phase)
			}
		}
	})

	// An earlier backup under the same prefix can leave a record behind. It is
	// no part of this group, so the lone agent must not wait for it to catch up.
	t.Run("record left by another member is not waited for", func(t *testing.T) {
		prefix := testPrefix(t)
		joinMember(t, prefix, "rs1", "preparing")

		b := newTestBarrier(t, prefix, "rs0", 1)
		released := advanceAsync(t, b, "running")

		select {
		case err := <-released:
			if err != nil {
				t.Fatalf("Advance: %v", err)
			}
		case <-time.After(waitTimeout):
			t.Fatal("Advance waited for a member outside the group of one")
		}
	})

	// The whole backup as the agent runs it: every phase, then a clean exit
	// that leaves the final phase on record for anyone reading the keyspace.
	t.Run("agent walks the backup and leaves its final phase behind", func(t *testing.T) {
		prefix := testPrefix(t)
		b := newBarrier(t, prefix, "rs0", 1)

		for _, phase := range backupPhases {
			if err := b.Advance(t.Context(), phase); err != nil {
				t.Fatalf("Advance(%q): %v", phase, err)
			}
		}

		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		phase, found, leased := memberKey(t, prefix, "rs0")
		if !found {
			t.Fatal("member key: gone, want the final phase still on record")
		}
		if phase != "done" {
			t.Errorf("phase: got %q, want %q", phase, "done")
		}
		if !leased {
			t.Error("member key outlives its lease, want it to lapse")
		}
	})
}

func TestAdvanceSelfLost(t *testing.T) {
	prefix := testPrefix(t)
	b := newTestBarrier(t, prefix, "rs0", 2)
	released := advanceAsync(t, b, "running")

	select {
	case err := <-released:
		t.Fatalf("Advance returned before the group was complete: %v", err)
	case <-time.After(waitTimeout):
	}

	// Our own lease expires: we are no longer a member of the group.
	revoke(t, b.sess.Lease())

	select {
	case err := <-released:
		if !errors.Is(err, ErrSelfLost) {
			t.Fatalf("Advance: got %v, want ErrSelfLost", err)
		}
	case <-time.After(waitTimeout):
		t.Fatal("Advance did not report the expired session")
	}
}

func TestAdvanceFinishedMemberLeaves(t *testing.T) {
	prefix := testPrefix(t)
	slow := joinMember(t, prefix, "rs1", "starting")
	finished := joinMember(t, prefix, "rs2", "done")

	b := newTestBarrier(t, prefix, "rs0", 3)
	released := advanceAsync(t, b, "done")

	select {
	case err := <-released:
		t.Fatalf("Advance returned while rs1 was still starting: %v", err)
	case <-time.After(waitTimeout):
	}

	// rs2 is done with the backup, and its key has since lapsed.
	revoke(t, finished)
	// rs1 catches up, so every member of the group has now reached "done".
	announce(t, prefix, "rs1", "done", slow)

	mustRelease(t, released, "Advance")
}

// TestClose covers what a member leaves behind, which is how the group tells a
// finished member from a failed one.
func TestClose(t *testing.T) {
	t.Run("finished member leaves its record to lapse", func(t *testing.T) {
		prefix := testPrefix(t)
		b := newBarrier(t, prefix, "rs0", 1)

		for _, phase := range backupPhases {
			if err := b.Advance(t.Context(), phase); err != nil {
				t.Fatalf("Advance(%q): %v", phase, err)
			}
		}

		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		// Still there for peers that are still finishing, and still on the
		// lease, so it goes by itself once the TTL is up.
		phase, found, leased := memberKey(t, prefix, "rs0")
		if !found {
			t.Fatal("member key: gone, want the final phase still on record")
		}
		if phase != "done" {
			t.Errorf("phase: got %q, want %q", phase, "done")
		}
		if !leased {
			t.Error("member key outlives its lease, want it to lapse")
		}
	})

	t.Run("member that bails takes its record with it", func(t *testing.T) {
		prefix := testPrefix(t)
		joinMember(t, prefix, "rs1", "starting")
		b := newBarrier(t, prefix, "rs0", 2)

		if err := b.Advance(t.Context(), "preparing"); err != nil {
			t.Fatalf("Advance: %v", err)
		}
		// Short of the last phase: this is a failure, and peers must find out.
		if err := b.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		if _, found, _ := memberKey(t, prefix, "rs0"); found {
			t.Fatal("member key: still there, want it gone")
		}
	})

	t.Run("peers waiting on a member that bails fail fast", func(t *testing.T) {
		prefix := testPrefix(t)
		// rs1 joins and then gives up before reaching any phase at all.
		quitter := newBarrier(t, prefix, "rs1", 2)

		b := newTestBarrier(t, prefix, "rs0", 2)
		released := advanceAsync(t, b, "running")

		select {
		case err := <-released:
			t.Fatalf("Advance returned while rs1 was still alive: %v", err)
		case <-time.After(waitTimeout):
		}

		if err := quitter.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		select {
		case err := <-released:
			if !errors.Is(err, ErrMemberLost) {
				t.Fatalf("Advance: got %v, want ErrMemberLost", err)
			}
		case <-time.After(waitTimeout):
			t.Fatal("Advance did not report the member that bailed")
		}
	})
}

// advanceAsync runs Advance in the background and reports its result.
func advanceAsync(t *testing.T, b *Barrier, phase string) <-chan error {
	t.Helper()

	released := make(chan error, 1)
	ctx := t.Context()
	go func() { released <- b.Advance(ctx, phase) }()

	return released
}

// mustRelease asserts that a pending Advance returns without error.
func mustRelease(t *testing.T, released <-chan error, what string) {
	t.Helper()

	select {
	case err := <-released:
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: barrier did not release", what)
	}
}

// testPrefix gives every test its own keyspace, wiped up front so leftovers
// from an earlier run cannot join the group under test.
func testPrefix(t *testing.T) string {
	t.Helper()

	prefix := "/test/" + t.Name() + "/"
	if _, err := ctlCli.Delete(t.Context(), prefix, clientv3.WithPrefix()); err != nil {
		t.Fatalf("reset keyspace %s: %v", prefix, err)
	}

	return prefix
}

func newEtcdClient(t *testing.T) *clientv3.Client {
	t.Helper()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   testEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("dial etcd client: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	return cli
}

// newTestBarrier builds a barrier and closes it when the test ends. Tests that
// close it themselves use newBarrier, since Close is called exactly once.
func newTestBarrier(t *testing.T, prefix, id string, size int) *Barrier {
	t.Helper()

	b := newBarrier(t, prefix, id, size)
	t.Cleanup(func() { _ = b.Close() })

	return b
}

func newBarrier(t *testing.T, prefix, id string, size int) *Barrier {
	t.Helper()

	b, err := New(t.Context(), newEtcdClient(t), Options{
		Prefix: prefix,
		ID:     id,
		Size:   size,
		Phases: backupPhases,
		TTL:    5 * time.Second,
	})
	if err != nil {
		t.Fatalf("new barrier for %s: %v", id, err)
	}

	return b
}

// joinMember stands up a synthetic member the test drives manually.
func joinMember(t *testing.T, prefix, id, phase string) clientv3.LeaseID {
	t.Helper()

	lease, err := ctlCli.Grant(t.Context(), 60)
	if err != nil {
		t.Fatalf("grant lease for %s: %v", id, err)
	}
	announce(t, prefix, id, phase, lease.ID)

	return lease.ID
}

// announce records that a synthetic member reached phase, the way Advance does.
func announce(t *testing.T, prefix, id, phase string, lease clientv3.LeaseID) {
	t.Helper()

	_, err := ctlCli.Put(t.Context(), prefix+id, phase, clientv3.WithLease(lease))
	if err != nil {
		t.Fatalf("announce %q for %s: %v", phase, id, err)
	}
}

// memberKey reports a member's phase, whether its key is there, and whether a
// lease still holds it.
func memberKey(t *testing.T, prefix, id string) (string, bool, bool) {
	t.Helper()

	resp, err := ctlCli.Get(t.Context(), prefix+id)
	if err != nil {
		t.Fatalf("get key for %s: %v", id, err)
	}
	if len(resp.Kvs) == 0 {
		return "", false, false
	}

	return string(resp.Kvs[0].Value), true, resp.Kvs[0].Lease != 0
}

func revoke(t *testing.T, lease clientv3.LeaseID) {
	t.Helper()

	if _, err := ctlCli.Revoke(t.Context(), lease); err != nil {
		t.Fatalf("revoke lease: %v", err)
	}
}
