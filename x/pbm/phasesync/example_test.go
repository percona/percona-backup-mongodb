package phasesync

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// ExampleBarrier_shardedCluster runs a backup across two shards and a config server.
// Each agent is a separate process in production, so each gets:
// - its own function below,
// - its own etcd client and
// - its own barrier.
// A different agent is the slowest in every phase, so the log shows that.
func ExampleBarrier_shardedCluster() {
	const (
		backupPrefix    = "pbmtest/example/backups/2026-09-14T12:00:00Z/"
		backupGroupSize = 3
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	agents := []struct {
		id  string
		run func(ctx context.Context, cli *clientv3.Client, prefix string, size int) error
	}{
		{"rs1", runAgentRS1},
		{"rs2", runAgentRS2},
		{"cfg", runAgentCfg},
	}

	// One client per agent: separate processes in production.
	clients := make([]*clientv3.Client, len(agents))
	defer func() {
		for _, cli := range clients {
			if cli != nil {
				_ = cli.Close()
			}
		}
	}()

	for i := range agents {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   testEndpoints,
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			fmt.Println("dial etcd:", err)
			return
		}
		clients[i] = cli
	}

	errs := make([]error, len(agents))
	var wg sync.WaitGroup
	for i, agent := range agents {
		wg.Go(func() {
			errs[i] = agent.run(ctx, clients[i], backupPrefix, backupGroupSize)
		})
	}
	wg.Wait()

	for i, agent := range agents {
		if errs[i] != nil {
			fmt.Printf("%s: %v\n", agent.id, errs[i])
			continue
		}
		fmt.Printf("%s finished all phases\n", agent.id)
	}

	// Output:
	// rs1: preparing took 1s, waiting for the rest of the group
	// rs2: preparing took 2s, waiting for the rest of the group
	// cfg: preparing took 3s, waiting for the rest of the group
	// cfg: starting took 1s, waiting for the rest of the group
	// rs1: starting took 2s, waiting for the rest of the group
	// rs2: starting took 3s, waiting for the rest of the group
	// rs2: running took 1s, waiting for the rest of the group
	// cfg: running took 2s, waiting for the rest of the group
	// rs1: running took 3s, waiting for the rest of the group
	// rs1: done took 1s, waiting for the rest of the group
	// rs2: done took 2s, waiting for the rest of the group
	// cfg: done took 3s, waiting for the rest of the group
	// rs1 finished all phases
	// rs2 finished all phases
	// cfg finished all phases
}

func runAgentRS1(ctx context.Context, cli *clientv3.Client, prefix string, size int) error {
	b, err := New(ctx, cli, Options{
		Prefix: prefix,
		ID:     "rs1",
		Size:   size,
		Phases: []string{"preparing", "starting", "running", "done"},
		TTL:    5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer b.Close()

	work("rs1", "preparing", 1*time.Second)
	if err := b.Advance(ctx, "preparing"); err != nil {
		return err
	}

	work("rs1", "starting", 2*time.Second)
	if err := b.Advance(ctx, "starting"); err != nil {
		return err
	}

	work("rs1", "running", 3*time.Second)
	if err := b.Advance(ctx, "running"); err != nil {
		return err
	}

	work("rs1", "done", 1*time.Second)
	return b.Advance(ctx, "done")
}

func runAgentRS2(ctx context.Context, cli *clientv3.Client, prefix string, size int) error {
	b, err := New(ctx, cli, Options{
		Prefix: prefix,
		ID:     "rs2",
		Size:   size,
		Phases: []string{"preparing", "starting", "running", "done"},
		TTL:    5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer b.Close()

	work("rs2", "preparing", 2*time.Second)
	if err := b.Advance(ctx, "preparing"); err != nil {
		return err
	}

	work("rs2", "starting", 3*time.Second)
	if err := b.Advance(ctx, "starting"); err != nil {
		return err
	}

	work("rs2", "running", 1*time.Second)
	if err := b.Advance(ctx, "running"); err != nil {
		return err
	}

	work("rs2", "done", 2*time.Second)
	return b.Advance(ctx, "done")
}

func runAgentCfg(ctx context.Context, cli *clientv3.Client, prefix string, size int) error {
	b, err := New(ctx, cli, Options{
		Prefix: prefix,
		ID:     "cfg",
		Size:   size,
		Phases: []string{"preparing", "starting", "running", "done"},
		TTL:    5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer b.Close()

	work("cfg", "preparing", 3*time.Second)
	if err := b.Advance(ctx, "preparing"); err != nil {
		return err
	}

	work("cfg", "starting", 1*time.Second)
	if err := b.Advance(ctx, "starting"); err != nil {
		return err
	}

	work("cfg", "running", 2*time.Second)
	if err := b.Advance(ctx, "running"); err != nil {
		return err
	}

	work("cfg", "done", 3*time.Second)
	return b.Advance(ctx, "done")
}

func work(id, phase string, d time.Duration) {
	time.Sleep(d)
	fmt.Printf("%s: %s took %s, waiting for the rest of the group\n", id, phase, d)
}

// ExampleBarrier_replicaSet runs the same backup on a single replica set.
func ExampleBarrier_replicaSet() {
	const (
		backupPrefix    = "pbmtest/example/backups/2026-09-14T13:00:00Z/"
		backupGroupSize = 1
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   testEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Println("dial etcd:", err)
		return
	}
	defer func() { _ = cli.Close() }()

	if err := runAgentRS0(ctx, cli, backupPrefix, backupGroupSize); err != nil {
		fmt.Printf("rs0: %v\n", err)
		return
	}
	fmt.Println("rs0 finished all phases")

	// Output:
	// rs0: preparing cleared at once, nobody else to wait for
	// rs0: starting cleared at once, nobody else to wait for
	// rs0: running cleared at once, nobody else to wait for
	// rs0: done cleared at once, nobody else to wait for
	// rs0 finished all phases
}

func runAgentRS0(ctx context.Context, cli *clientv3.Client, prefix string, size int) error {
	b, err := New(ctx, cli, Options{
		Prefix: prefix,
		ID:     "rs0",
		Size:   size,
		Phases: []string{"preparing", "starting", "running", "done"},
		TTL:    5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer b.Close()

	if err := b.Advance(ctx, "preparing"); err != nil {
		return err
	}
	fmt.Println("rs0: preparing cleared at once, nobody else to wait for")

	if err := b.Advance(ctx, "starting"); err != nil {
		return err
	}
	fmt.Println("rs0: starting cleared at once, nobody else to wait for")

	if err := b.Advance(ctx, "running"); err != nil {
		return err
	}
	fmt.Println("rs0: running cleared at once, nobody else to wait for")

	if err := b.Advance(ctx, "done"); err != nil {
		return err
	}
	fmt.Println("rs0: done cleared at once, nobody else to wait for")

	return nil
}
