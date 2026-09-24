// Package disco provides serf-based cluster membership discovery. It runs a
// serf node, forwards membership changes to the status service, and broadcasts
// this agent's status tags to peers.
package disco

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hashicorp/serf/serf"

	"github.com/percona/percona-backup-mongodb/x/pbm/status"
)

const serfJoinInterval = 2 * time.Second

// Config holds Serf discovery configuration shared by all agents.
type Config struct {
	SerfPort int
	// SerfJoin lists seed addresses (host:port) to gossip with on startup.
	// Reaching any one is enough; serf discovers the rest. Empty starts a new
	// cluster.
	SerfJoin []string
}

// Discovery runs serf node and its membership event loop.
type Discovery struct {
	serf *serf.Serf
	sync chan<- status.StatusEvent
	done chan struct{}
}

// Start creates the serf node, starts the membership event loop, and
// joins the seed if one is configured. Membership changes are forwarded to sync
// channel for the status service. The caller owns shutdown via Stop().
func Start(
	ctx context.Context,
	name string,
	cfg Config,
	sync chan<- status.StatusEvent,
) (*Discovery, error) {
	// todo: improve
	// buffered so a momentarily slow consumer can't block serf's dispatcher
	eventCh := make(chan serf.Event, 64)

	sc := serf.DefaultConfig()
	sc.NodeName = name
	sc.EventCh = eventCh
	sc.MemberlistConfig.BindAddr = "0.0.0.0"
	if cfg.SerfPort != 0 {
		sc.MemberlistConfig.BindPort = cfg.SerfPort
	}
	sc.Logger = log.New(os.Stdout, "serf ", log.LstdFlags) // todo

	s, err := serf.Create(sc)
	if err != nil {
		return nil, fmt.Errorf("create serf: %w", err)
	}

	d := &Discovery{serf: s, sync: sync, done: make(chan struct{})}
	go d.eventLoop(ctx, eventCh)

	if len(cfg.SerfJoin) > 0 {
		d.retryJoin(ctx, cfg.SerfJoin)
	}

	return d, nil
}

// retryJoin keeps attempting to gossip with the seed peers until this node is
// part of a multi-member cluster.
func (d *Discovery) retryJoin(ctx context.Context, peers []string) {
	t := time.NewTicker(serfJoinInterval)
	defer t.Stop()

	for {
		if _, err := d.serf.Join(peers, true); err != nil {
			log.Printf("serf: join attempt failed (%v); retrying in %s", err, serfJoinInterval)
		}
		if n := len(d.serf.Members()); n > 1 {
			log.Printf("serf: clustered with %d members", n)
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-d.serf.ShutdownCh():
			return
		case <-t.C:
		}
	}
}

// Publish broadcasts the agent's status tags to the cluster as serf member tags.
// It publishes all the tags, not just patch.
func (d *Discovery) Publish(tags map[string]string) error {
	return d.serf.SetTags(tags)
}

// eventLoop processes cluster membership forwarding each change to the status service.
func (d *Discovery) eventLoop(ctx context.Context, eventCh <-chan serf.Event) {
	defer close(d.done)

	shutdownCh := d.serf.ShutdownCh()
	for {
		select {
		case e := <-eventCh:
			me, ok := e.(serf.MemberEvent)
			if !ok {
				// ignore user events and queries
				continue
			}
			d.handleMemberEvent(ctx, me)
		case <-shutdownCh:
			return
		}
	}
}

func (d *Discovery) handleMemberEvent(ctx context.Context, me serf.MemberEvent) {
	kind, alive, ok := classifyEvent(me.Type)
	if !ok {
		return // just ignore it for now, but improve it
	}

	for _, m := range me.Members {
		log.Printf("serf membership: %s name=%s addr=%s:%d",
			me.Type, m.Name, m.Addr, m.Port)

		ev := status.StatusEvent{
			Kind:  kind,
			Name:  m.Name,
			Addr:  m.Addr.String(),
			Port:  int(m.Port),
			Alive: alive,
		}
		if alive {
			ev.Payload = m.Tags
		}

		select {
		case d.sync <- ev:
			// todo: turn on logging here
		case <-ctx.Done():
			return
		}
	}
}

// classifyEvent maps a serf member event type to a status EventKind.
func classifyEvent(t serf.EventType) (kind status.EventKind, alive, ok bool) {
	switch t {
	case serf.EventMemberJoin:
		return status.MemberUp, true, true
	case serf.EventMemberUpdate:
		return status.MemberUpdate, true, true
	case serf.EventMemberFailed:
		return status.MemberFailed, false, true
	case serf.EventMemberLeave, serf.EventMemberReap:
		// todo: improve this
		return status.MemberGone, false, true
	default:
		return 0, false, false
	}
}

// Stop leaves the cluster gracefully so peers record a clean leave rather than a
// failure, then tears the node down and waits for the event loop to exit.
func (d *Discovery) Stop() error {
	leaveErr := d.serf.Leave()
	shutErr := d.serf.Shutdown()
	<-d.done
	return errors.Join(leaveErr, shutErr)
}
