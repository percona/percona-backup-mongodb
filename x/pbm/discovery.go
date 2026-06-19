package pbm

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hashicorp/serf/serf"
)

const serfJoinInterval = 2 * time.Second

// discovery runs serf node and its membership event loop.
type discovery struct {
	serf *serf.Serf
	done chan struct{}
}

// startDiscovery creates the serf node, starts the membership event loop, and
// joins the seed if one is configured. The caller owns shutdown via stop().
func startDiscovery(ctx context.Context, name string, cfg DiscoConfig) (*discovery, error) {
	// buffered so a momentarily slow consumer can't block serf's dispatcher.
	eventCh := make(chan serf.Event, 64)

	sc := serf.DefaultConfig()
	sc.NodeName = name
	sc.EventCh = eventCh
	sc.MemberlistConfig.BindAddr = "0.0.0.0"
	if cfg.SerfPort != 0 {
		sc.MemberlistConfig.BindPort = cfg.SerfPort
	}
	sc.Logger = log.New(os.Stdout, "serf ", log.LstdFlags) //todo

	s, err := serf.Create(sc)
	if err != nil {
		return nil, fmt.Errorf("create serf: %w", err)
	}

	d := &discovery{serf: s, done: make(chan struct{})}
	go d.eventLoop(eventCh)

	if len(cfg.SerfJoin) > 0 {
		d.retryJoin(ctx, cfg.SerfJoin)
	}

	return d, nil
}

// retryJoin keeps attempting to gossip with the seed peers until this node is
// part of a multi-member cluster.
func (d *discovery) retryJoin(ctx context.Context, peers []string) {
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

// eventLoop process cluster membership changes until the serf node shuts down.
func (d *discovery) eventLoop(eventCh <-chan serf.Event) {
	defer close(d.done)

	shutdownCh := d.serf.ShutdownCh()
	for {
		select {
		case e := <-eventCh:
			me, ok := e.(serf.MemberEvent)
			if !ok {
				// ignore user events and queries for now
				continue
			}
			switch me.Type {
			case serf.EventMemberJoin, serf.EventMemberLeave, serf.EventMemberFailed:
				for _, m := range me.Members {
					log.Printf("serf membership: %s name=%s addr=%s:%d",
						me.Type, m.Name, m.Addr, m.Port)
				}
			default:
				// EventMemberUpdate/Reap not interesting yet.
			}
		case <-shutdownCh:
			return
		}
	}
}

// stop leaves the cluster gracefully so peers record a clean leave rather than a
// failure, then tears the node down and waits for the event loop to exit.
func (d *discovery) stop() error {
	leaveErr := d.serf.Leave()
	shutErr := d.serf.Shutdown()
	<-d.done
	return errors.Join(leaveErr, shutErr)
}
