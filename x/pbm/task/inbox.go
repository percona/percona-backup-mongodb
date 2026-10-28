package task

import (
	"context"
	"encoding/json"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
)

const watchRetry = 5 * time.Second

// Backuper runs the backup when inbox item is scheduled for this agent.
type Backuper interface {
	Run(ctx context.Context, name string, groupSize int, isLeader bool, opts json.RawMessage) error
}

// Inbox watches one agent's inbox and runs the items it finds there.
// Every agent (worker and ctrl-agent) runs one.
type Inbox struct {
	lg      *zap.Logger
	ccDB    *clientv3.Client
	agentID string
	backup  Backuper
}

// NewInbox creates the inbox watcher for the agent named agentID.
func NewInbox(lg *zap.Logger, ccDB *clientv3.Client, agentID string, backup Backuper) *Inbox {
	return &Inbox{
		lg:      lg.Named("inbox"),
		ccDB:    ccDB,
		agentID: agentID,
		backup:  backup,
	}
}

// Run watches the inbox until ctx is canceled.
func (i *Inbox) Run(ctx context.Context) error {
	for {
		if err := i.watch(ctx); err != nil {
			i.lg.Warn("watch inbox, retrying", zap.Duration("retry_in", watchRetry), zap.Error(err))
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(watchRetry):
		}
	}
}

func (i *Inbox) watch(ctx context.Context) error {
	wch := i.ccDB.Watch(ctx, agentInboxPrefix(i.agentID), clientv3.WithPrefix())

	for wresp := range wch {
		if err := wresp.Err(); err != nil {
			return errors.Wrap(err, "watch inbox")
		}
		for _, ev := range wresp.Events {
			// only process new items
			if ev.Type != clientv3.EventTypePut {
				continue
			}
			i.handle(ctx, ev.Kv.Value, clientv3.LeaseID(ev.Kv.Lease))
		}
	}

	return ctx.Err()
}

// handle decodes one item and works on it. It blocks the watch loop until the
// task is done: an agent runs one task at a time.
func (i *Inbox) handle(ctx context.Context, value []byte, lease clientv3.LeaseID) {
	a := InboxItem{}
	if err := json.Unmarshal(value, &a); err != nil {
		i.lg.Error("unmarshal item", zap.Error(err))
		return
	}

	switch a.Type {
	case TaskBackup:
		i.runBackup(ctx, a, lease)
	default:
		i.lg.Warn("unknown task type", zap.String("type", string(a.Type)))
	}
}

// runBackup keeps the task's lease alive for as long as the work takes.
func (i *Inbox) runBackup(ctx context.Context, a InboxItem, lease clientv3.LeaseID) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	lg := i.lg.With(zap.String("backup", a.Task), zap.Bool("leader", a.IsLeader))

	i.keepAlive(ctx, lg, lease)

	lg.Info("starting backup")
	if err := i.backup.Run(ctx, a.Task, a.GroupSize, a.IsLeader, a.Options); err != nil {
		lg.Error("backup failed", zap.Error(err))
		return
	}
	lg.Info("backup is processed")
}

// keepAlive renews the task's lease until ctx is canceled. Every participant
// renews the same lease, so the task survives as long as any of them works
// on it.
func (i *Inbox) keepAlive(ctx context.Context, lg *zap.Logger, lease clientv3.LeaseID) {
	if lease == clientv3.NoLease {
		return
	}

	ch, err := i.ccDB.KeepAlive(ctx, lease)
	if err != nil {
		lg.Error("keep alive lease", zap.Int64("lease", int64(lease)), zap.Error(err))
		return
	}

	go func() {
		// otherwise the client stops renewing
		for range ch {
		}
	}()
}
