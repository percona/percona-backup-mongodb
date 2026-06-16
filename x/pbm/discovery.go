package pbm

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/hashicorp/serf/serf"
)

// discovery runs serf node and its membership event loop.
type discovery struct {
	serf *serf.Serf
	done chan struct{}
}

// startDiscovery creates the serf node, starts the membership event loop, and
// joins the seed if one is configured. The caller owns shutdown via stop().
func startDiscovery(name string, cfg DiscoConfig) (*discovery, error) {
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

	if cfg.SerfJoin != "" {
		n, err := s.Join([]string{cfg.SerfJoin}, true)
		if err != nil {
			log.Printf("serf join %q failed, continuing: %v", cfg.SerfJoin, err)
		} else {
			log.Printf("serf joined %d node(s) via %q", n, cfg.SerfJoin)
		}
	}

	return d, nil
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
