// Package phasesync implements a lease-backed, multi-phase barrier: a group of
// agents each announce the phase they have reached, and nobody leaves a phase
// until every member of the group has reached it.
package phasesync

import (
	"context"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
)

var (
	ErrMemberLost = errors.New("member lost")
	ErrSelfLost   = errors.New("own session expired")
)

// DefaultTTL is how long after a member stops renewing its lease it is declared lost.
const DefaultTTL = 10 * time.Second

// Barrier is one member's handle on a group activity: for a group of agents that means that all agents stop
// at phase point and do not proceed until all other agents reach this phase point.

// Each member keeps one key, <prefix><id>, holding the furthest phase it has
// announced. Its lease holds the key, so that other agent(s) noticed if the member dies.

// It holds a lease, so it must be closed, and is meant to be used by a single goroutine.
type Barrier struct {
	sess    *concurrency.Session
	prefix  string // e.g. "/jobs/job-42/", always with a trailing slash
	id      string // unique per agent, e.g. hostname:port
	size    int    // expected group size: number of agents
	phases  []string
	rank    map[string]int
	reached int
}

type Options struct {
	// Prefix namespaces one phase, e.g. "/backups/2026-09-11T12:00:00Z/".
	Prefix string
	// ID is unique per agent.
	ID string
	// Size is how many agents take part. Defaults to 1 (e.g. for RS).
	Size int
	// Phases in the order they occur, e.g. {"starting", "working", "done"}.
	Phases []string
	// TTL defaults to DefaultTTL
	TTL time.Duration
}

// New creates Barrier for specified opts.
// ctx bounds Barrier, not just this call: canceling it drops the lease, and the group sees this member leave.
func New(ctx context.Context, cli *clientv3.Client, opts Options) (*Barrier, error) {
	switch {
	case cli == nil:
		return nil, errors.New("nil etcd client")
	case opts.Prefix == "":
		return nil, errors.New("empty prefix")
	case opts.ID == "":
		return nil, errors.New("empty member id")
	case opts.Size < 0:
		return nil, errors.Errorf("group size %d must be positive", opts.Size)
	case len(opts.Phases) == 0:
		return nil, errors.New("no phases")
	}

	size := opts.Size
	if size == 0 {
		size = 1
	}
	ttl := opts.TTL
	if ttl < 3*time.Second {
		ttl = DefaultTTL
	}

	rank := make(map[string]int, len(opts.Phases))
	for i, p := range opts.Phases {
		rank[p] = i
	}

	sess, err := concurrency.NewSession(
		cli,
		concurrency.WithTTL(int(ttl.Seconds())),
		concurrency.WithContext(ctx),
	)
	if err != nil {
		return nil, errors.Wrap(err, "new etcd session")
	}

	b := &Barrier{
		sess:    sess,
		prefix:  withSlash(opts.Prefix),
		id:      opts.ID,
		size:    size,
		phases:  opts.Phases,
		rank:    rank,
		reached: -1,
	}

	if _, err := cli.Put(ctx, b.key(), "", clientv3.WithLease(sess.Lease())); err != nil {
		_ = sess.Close()
		return nil, errors.Wrapf(err, "join %q", b.prefix)
	}

	return b, nil
}

// Close takes this member out of the group.
// Leaving before the last phase means failure: the lease is revoked,
// peers waiting on a phase this member will get ErrMemberLost.
// For agent that finished, Close only stops the renewals.
func (b *Barrier) Close() error {
	if b.reached != len(b.phases)-1 {
		return b.sess.Close()
	}

	b.sess.Orphan()
	return nil
}

// Advance publishes that this agent has reached phase, then blocks until every
// member of the group has reached that phase. It returns when the
// whole group is lined up, so the agent can start the next phase's work.
func (b *Barrier) Advance(ctx context.Context, phase string) error {
	target, ok := b.rank[phase]
	if !ok {
		return fmt.Errorf("unknown phase %q", phase)
	}
	if target <= b.reached {
		return errors.Errorf("phase %q is not ahead of %q", phase, b.phases[b.reached])
	}
	cli := b.sess.Client()

	if _, err := cli.Put(ctx, b.key(), phase, clientv3.WithLease(b.sess.Lease())); err != nil {
		return errors.Wrapf(err, "announce phase %q", phase)
	}
	b.reached = target

	// snapshot the group and watch all since then
	resp, err := cli.Get(ctx, b.prefix, clientv3.WithPrefix())
	if err != nil {
		return errors.Wrapf(err, "get group state under %q", b.prefix)
	}
	state := make(map[string]int, b.size)
	for _, kv := range resp.Kvs {
		state[b.memberID(string(kv.Key))] = b.rankOf(string(kv.Value))
	}
	if b.allReached(state, target) {
		return nil
	}

	wch := cli.Watch(ctx, b.prefix, clientv3.WithPrefix(), clientv3.WithRev(resp.Header.Revision+1))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.sess.Done():
			return ErrSelfLost
		case wr, ok := <-wch:
			if !ok {
				return errors.New("watch channel closed")
			}
			if err := wr.Err(); err != nil {
				return errors.Wrapf(err, "watch prefix %q", b.prefix)
			}
			for _, ev := range wr.Events {
				key := string(ev.Kv.Key)
				id := b.memberID(key)
				switch ev.Type {
				case clientv3.EventTypePut:
					state[id] = b.rankOf(string(ev.Kv.Value))
				case clientv3.EventTypeDelete:
					if key == b.key() {
						return ErrSelfLost
					}
					// leaving is only allowed at the last phase, where the member is finished
					if target == len(b.phases)-1 && state[id] >= target {
						continue
					}
					return fmt.Errorf("%w: %s", ErrMemberLost, id)
				}
			}
			if b.allReached(state, target) {
				return nil
			}
		}
	}
}

// allReached checks if all agents reached the target phase.
func (b *Barrier) allReached(state map[string]int, target int) bool {
	n := 0
	for _, r := range state {
		// there should be >= because agent can still draining
		// its historical revisions of the state
		if r >= target {
			n++
		}
	}

	return n >= b.size
}

func (b *Barrier) key() string { return b.prefix + b.id }

func (b *Barrier) memberID(key string) string { return strings.TrimPrefix(key, b.prefix) }

func (b *Barrier) rankOf(v string) int {
	if i, ok := b.rank[v]; ok {
		return i
	}

	return -1 // no phase announced yet, or one we do not know
}

func withSlash(prefix string) string {
	if strings.HasSuffix(prefix, "/") {
		return prefix
	}

	return prefix + "/"
}
